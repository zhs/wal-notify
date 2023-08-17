package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
)

const (
	standbyTimeout = 10 * time.Second
)

// LogicalMessage is used as an argument to handlers
type LogicalMessage struct {
	Message    pglogrepl.Message
	Relations  map[uint32]*pglogrepl.RelationMessage
	WALPointer pglogrepl.LSN
}

// Handler is processing function for handling inserted records
type Handler func(ctx context.Context, table string, values map[string][]byte) error

func (n *Notifier) listen(ctx context.Context, handler Handler) error {
	var clientXLogPos pglogrepl.LSN

	standbyMessageTimeout := standbyTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := map[uint32]*pglogrepl.RelationMessage{}
	txchecker := &txChecker{}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// update standby status
		if time.Now().After(nextStandbyMessageDeadline) {
			if err := ackMessage(ctx, n.conn, clientXLogPos); err != nil {
				return fmt.Errorf("WAL listener send standby status update failed: %w", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		rawMsg, err := n.conn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Error().Err(err).Msg("receive message failed")
			time.Sleep(1 * time.Second)
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v\n", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Error().
				Interface("raw message", rawMsg).
				Type("message type", rawMsg).
				Msg("received unexpected message")
			time.Sleep(1 * time.Second)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse XLogData failed: %w", err)
			}
			lMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical replication message failed: %w", err)
			}

			logicalMessage := &LogicalMessage{
				Message:    lMsg,
				Relations:  relations,
				WALPointer: xld.WALStart,
			}

			// handling event from the DB
			_, err = internalHandler(ctx, logicalMessage, handler)
			if err != nil {
				log.Err(err).Msg("event handler")
				txchecker.Clear()
				continue
			}

			// if event ok then ack
			if txchecker.Inserted(logicalMessage) {
				clientXLogPos = xld.WALStart
				if err := ackMessage(ctx, n.conn, xld.WALStart); err != nil {
					return fmt.Errorf("WAL listener send standby status update failed: %w", err)
				}
			}
		}
	}
}

type txChecker struct {
	started  bool
	inserted bool
}

func (c *txChecker) Inserted(lm *LogicalMessage) bool {
	switch lm.Message.(type) {
	case *pglogrepl.BeginMessage:
		c.Clear()
		c.started = true
	case *pglogrepl.InsertMessage:
		if c.started {
			c.inserted = true
		}
	case *pglogrepl.CommitMessage:
		if c.started && c.inserted {
			c.Clear()
			return true
		}
	}

	return false
}

func (c *txChecker) Clear() {
	c = &txChecker{}
}

func ackMessage(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn})
}
