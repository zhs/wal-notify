package wal

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgconn"
)

var Validator = validator.New()

type Options struct {
	DBAddr       string `validate:"required"`
	TableName    string `validate:"required"`
	PubName      string
	SlotName     string
	OutputPlugin string
}

type Notifier struct {
	opts *Options
	conn *pgconn.PgConn
}

func NewNotifier(opts *Options) (*Notifier, error) {
	err := Validator.Struct(opts)
	if err != nil {
		return nil, err
	}

	n := Notifier{
		opts: opts,
	}

	return &n, nil
}

func (n *Notifier) Listen(ctx context.Context, handler Handler) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	conn, err := newWALConnection(ctx, n.opts)
	if err != nil {
		return err
	}

	n.conn = conn

	if err = n.listen(ctx, handler); err != nil {
		return fmt.Errorf("error start WAL listener: %w", err)
	}

	return nil
}
