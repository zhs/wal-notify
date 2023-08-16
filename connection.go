package wal

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func newWALConnection(ctx context.Context, opts *Options) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, opts.DBAddr)
	if err != nil {
		return nil, fmt.Errorf("error to connect to DB: %w", err)
	}

	// ensure publication exists
	sqlDropPub := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", opts.PubName)
	if _, err := conn.Exec(ctx, sqlDropPub).ReadAll(); err != nil {
		return nil, fmt.Errorf("failed to drop publication: %w", err)
	}

	// create publication for table (warn: current pub only for INSERT)
	sqlCreatePub := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'INSERT');", opts.PubName, opts.TableName)
	if _, err := conn.Exec(ctx, sqlCreatePub).ReadAll(); err != nil {
		return nil, fmt.Errorf("failed to create publication: %w", err)
	}

	// create replication slot server
	if _, err = pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		opts.SlotName,
		opts.OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false}); err != nil {
		// TODO: check first if the slot already exists:
		// SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'replication_slot';
		// and if not exists then return creation error:
		// return nil, fmt.Errorf("failed to create a replication slot: %w", err)
	}

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", opts.PubName)}

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		opts.SlotName,
		0,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return nil, fmt.Errorf("failed to establish start replication: %w", err)
	}

	return conn, nil
}
