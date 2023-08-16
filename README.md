# WAL-Notifier

The notifier is used for processing INSERTED records into the Postgres table.

Creating a new notifier:

```go

    defaultOpts := &wal.Options{
        DBAddr:       "postgres://[user]:[password]@127.0.0.1:5432/[db]?replication=database",
        TableName:    "[table_name]",
        PubName:      "pub",
        SlotName:     "replication_slot",
        OutputPlugin: "pgoutput",
    }

    // create new instance of WAL-Notifier
    n, err := wal.NewNotifier(defaultOpts)

    // handler for processing record data.
    handler := func(ctx context.Context, table string, values map[string][]byte, lsn pglogrepl.LSN) error {
        // "values" is a map of fields of DB record: [column_name]:[data]
        // "lsn" is record ID	
        ...
    }

    // run notifier
    err = n.Listen(context.TODO(), handler)
```