# WAL-Notifier 0.1

The notifier is used for processing INSERTED records into the Postgres table.

Returning an error from the handler is only necessary as a guarantee that processing will continue from that event after the notifiers crash.
All error handling should be done on the handler side, since events will continue to arrive despite the returned error.

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
    handler := func(ctx context.Context, table string, values map[string][]byte) error {
        // "values" is a map of fields of DB record: [column_name]:[data]
        ...
    }

    // run notifier
    err = n.Listen(context.TODO(), handler)
```