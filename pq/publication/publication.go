package publication

import (
	"context"
	goerrors "errors"
	"fmt"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorPublicationIsNotExists = goerrors.New("publication is not exists")
)

var typeMap = pgtype.NewMap()

type Publication struct {
	conn pq.Connection
	cfg  Config
}

func New(cfg Config, conn pq.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (c *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorPublicationIsNotExists) || !c.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "publication info")
		}
	} else {
		logger.Warn("publication already exists")
		return info, nil
	}

	resultReader := c.conn.Exec(ctx, c.cfg.createQuery())
	_, err = resultReader.ReadAll()
	if err != nil {
		var pgErr *pgconn.PgError
		if goerrors.As(err, &pgErr) && pgErr.Code == "42710" {
			info, inspectErr := c.Info(ctx)
			if inspectErr != nil {
				return nil, errors.Wrap(inspectErr, "publication info after concurrent create")
			}
			return info, nil
		}
		return nil, errors.Wrap(err, "publication create result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication create result reader close")
	}

	logger.Info("publication created", "name", c.cfg.Name)

	info, err = c.Info(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "publication info after create")
	}
	return info, nil
}

func (c *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := c.conn.Exec(ctx, c.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "42703" {
			return nil, ErrorPublicationIsNotExists
		}
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorPublicationIsNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication info result reader close")
	}

	publicationInfo, err := decodePublicationInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "publication info result decode")
	}

	return publicationInfo, nil
}

func decodePublicationInfoResult(result *pgconn.Result) (*Config, error) {
	if len(result.Rows) == 0 {
		return nil, ErrorPublicationIsNotExists
	}

	var publicationConfig Config
	for rowIndex, row := range result.Rows {
		var table Table
		for i, fd := range result.FieldDescriptions {
			v, err := decodeTextColumnData(row[i], fd.DataTypeOID)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue
			}

			switch fd.Name {
			case "pubname":
				publicationConfig.Name = v.(string)
			case "puballtables":
				publicationConfig.AllTables = v.(bool)
			case "pubinsert":
				if rowIndex == 0 && v.(bool) {
					publicationConfig.Operations = append(publicationConfig.Operations, OperationInsert)
				}
			case "pubupdate":
				if rowIndex == 0 && v.(bool) {
					publicationConfig.Operations = append(publicationConfig.Operations, OperationUpdate)
				}
			case "pubdelete":
				if rowIndex == 0 && v.(bool) {
					publicationConfig.Operations = append(publicationConfig.Operations, OperationDelete)
				}
			case "pubtruncate":
				if rowIndex == 0 && v.(bool) {
					publicationConfig.Operations = append(publicationConfig.Operations, OperationTruncate)
				}
			case "pubviaroot":
				publicationConfig.PublishViaPartitionRoot = v.(bool)
			case "pubschemas":
				publicationConfig.SchemaTables = v.(bool)
			case "schemaname":
				table.Schema = v.(string)
			case "tablename":
				table.Name = v.(string)
			case "columns":
				columns, err := stringArray(v)
				if err != nil {
					return nil, err
				}
				table.Columns = columns
			case "columns_specified":
				table.ColumnsSpecified = v.(bool)
			case "row_filter":
				table.RowFilter = v.(string)
			case "replica_identity":
				table.ReplicaIdentity = mapReplicaIdentity(v)
			case "replica_identity_index":
				table.ReplicaIdentityIndex = v.(string)
			}
		}
		if table.Name != "" {
			table.Partitioned = publicationConfig.PublishViaPartitionRoot
			publicationConfig.Tables = append(publicationConfig.Tables, table)
		}
	}

	return &publicationConfig, nil
}

func stringArray(value any) ([]string, error) {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...), nil
	case []any:
		result := make([]string, len(values))
		for i, value := range values {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("publication column %d has type %T", i, value)
			}
			result[i] = text
		}
		return result, nil
	default:
		return nil, fmt.Errorf("publication columns have type %T", value)
	}
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
