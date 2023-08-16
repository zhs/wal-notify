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
type Handler func(ctx context.Context, table string, values map[string][]byte, lsn pglogrepl.LSN) error

func (n *Notifier) listen(ctx context.Context, handler Handler) error {
	var clientXLogPos pglogrepl.LSN

	standbyMessageTimeout := standbyTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := map[uint32]*pglogrepl.RelationMessage{}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(
				ctx,
				n.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("WAL listener SendStandbyStatusUpdate failed: %w", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		rawMsg, err := n.conn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Error().Err(err).Msg("ReceiveMessage failed")
			time.Sleep(1 * time.Second)
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatal().Interface("err msg", errMsg).Msg("received Postgres WAL error")
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
			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical replication message failed: %w", err)
			}

			// handling event from the DB
			lsn, err := internalHandler(ctx, &LogicalMessage{
				Message:    logicalMsg,
				Relations:  relations,
				WALPointer: xld.WALStart,
			}, handler)
			if err != nil {
				n.errs <- fmt.Errorf("error in event handler: %w", err)
				continue
			}

			clientXLogPos = lsn

			//
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				n.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("WAL listener SendStandbyStatusUpdate failed: %w", err)
			}
		}
	}
}
