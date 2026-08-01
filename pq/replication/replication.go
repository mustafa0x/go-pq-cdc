package replication

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Replication struct {
	conn pq.Connection
}

func New(conn pq.Connection) *Replication {
	return &Replication{conn: conn}
}

func (r *Replication) Start(publicationName, slotName string, startLSN pq.LSN, protoVersion int, messages bool) error {
	pluginArguments := []string{"proto_version " + pq.QuoteLiteral(strconv.Itoa(protoVersion))}

	if protoVersion >= 2 {
		pluginArguments = append(pluginArguments, "streaming 'true'")
	}
	if messages {
		pluginArguments = append(pluginArguments, "messages 'true'")
	}

	pluginArguments = append(pluginArguments, "publication_names "+pq.QuoteLiteral(publicationName))

	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s)", pq.QuoteIdentifier(slotName), startLSN, strings.Join(pluginArguments, ", "))
	r.conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	if err := r.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func (r *Replication) Test(ctx context.Context) error {
	var (
		nextTli         int64
		nextTliStartPos pq.LSN
		commandErr      error
	)
	for {
		msg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receive replication startup message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse, *pgproto3.CommandComplete:
		case *pgproto3.ErrorResponse:
			commandErr = pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.CopyBothResponse:
			if commandErr != nil {
				return commandErr
			}
			return nil
		case *pgproto3.RowDescription:
			return errors.New("received RowDescription in logical replication startup")
		case *pgproto3.DataRow:
			if cnt := len(msg.Values); cnt != 2 {
				return fmt.Errorf("expected next_tli and next_tli_startpos, got %d fields", cnt)
			}
			nextTli, err = strconv.ParseInt(string(msg.Values[0]), 10, 64)
			if err != nil {
				return err
			}
			nextTliStartPos, err = pq.ParseLSN(string(msg.Values[1]))
			if err != nil {
				return err
			}
		case *pgproto3.ReadyForQuery:
			if commandErr != nil {
				return commandErr
			}
			if nextTli > 0 && nextTliStartPos > 0 {
				return errors.New("start replication with a switch point")
			}
			return errors.New("replication start ended before entering copy mode")
		default:
			return fmt.Errorf("unexpected response type: %T", msg)
		}
	}
}
