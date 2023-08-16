package wal

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog/log"
)

func internalHandler(ctx context.Context, logicalMessage *LogicalMessage, h Handler) (pglogrepl.LSN, error) {
	switch lm := logicalMessage.Message.(type) {
	case *pglogrepl.RelationMessage:
		logicalMessage.Relations[lm.RelationID] = lm

	case *pglogrepl.InsertMessage:
		rel, ok := logicalMessage.Relations[lm.RelationID]
		if !ok {
			return 0, fmt.Errorf("unknown relation ID %d", lm.RelationID)
		}
		values := make(map[string][]byte, len(lm.Tuple.Columns))
		for idx, col := range lm.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case pglogrepl.TupleDataTypeNull:
				values[colName] = []byte{}
			case pglogrepl.TupleDataTypeText:
				values[colName] = col.Data
			default:
				log.Warn().Uint8("data type", col.DataType).Msg("unexpected data type")
			}
		}

		err := h(ctx, rel.RelationName, values, logicalMessage.WALPointer)
		if err != nil {
			return 0, err
		}
	}

	return logicalMessage.WALPointer, nil
}
