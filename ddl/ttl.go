package ddl

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
)

const (
	ttlRowTimeColumnName      = "_tidb_row_time"
	ttlCurrentPartitionName   = "current"
	ttlRetentionPartitionName = "retention"

	ttlCurrentPartitionIndex   = 0
	ttlRetentionPartitionIndex = 1
)

// buildTableTTLPartitionInfo builds partition info for TTL table
func buildTableTTLPartitionInfo() *model.PartitionInfo {
	return &model.PartitionInfo{
		Type:    model.PartitionTypeTTL,
		Expr:    "TTL",
		Columns: []model.CIStr{model.NewCIStr(ttlRowTimeColumnName)},
		Enable:  true,
		Definitions: []model.PartitionDefinition{
			{
				Name:    model.NewCIStr(ttlCurrentPartitionName),
				Comment: "current writing partition",
			},
			{
				Name:    model.NewCIStr(ttlRetentionPartitionName),
				Comment: "retention partition",
			},
		},
		Num: 2,
	}
}

func generateTTLPartitionColumn(id int64, offset int) *model.ColumnInfo {
	col := &model.ColumnInfo{
		ID:                  id,
		Offset:              offset,
		GeneratedExprString: "CURRENT_TIMESTAMP",
		GeneratedStored:     true,
		Name:                model.NewCIStr(ttlRowTimeColumnName),
		Version:             model.CurrLatestColumnInfoVersion,
		State:               model.StatePublic,
	}
	col.Tp = mysql.TypeTimestamp
	col.Flag = mysql.BinaryFlag | mysql.NotNullFlag | mysql.NoDefaultValueFlag
	col.Flen = types.UnspecifiedLength
	col.Decimal = types.UnspecifiedLength
	setCharsetCollationFlenDecimal(&col.FieldType, charset.CharsetBin, charset.CharsetBin)
	return col
}

func rewriteTTLTableInfo(tbInfo *model.TableInfo) error {
	if !tbInfo.TTLByRow {
		for _, column := range tbInfo.Columns {
			if column.Name.L == ttlRowTimeColumnName {
				return errors.Errorf("column `%s` is reserved for ttl partition table")
			}
		}
		lastColumnOffset := tbInfo.Columns[len(tbInfo.Columns)-1].Offset
		tbInfo.MaxColumnID++
		tbInfo.Columns = append(tbInfo.Columns, generateTTLPartitionColumn(tbInfo.MaxColumnID, lastColumnOffset+1))
		tbInfo.Partition = buildTableTTLPartitionInfo()
	}
	return nil
}

func isTTLPartition(partitionInfo *model.PartitionInfo) bool {
	return partitionInfo.Type == model.PartitionTypeTTL
}

func isTTLPartitionTable(tbInfo *model.TableInfo) bool {
	return tbInfo.Partition != nil && isTTLPartition(tbInfo.Partition)
}

func checkAlterOnTTLPartition(tbInfo *model.TableInfo, partitionName string) error {
	if !isTTLPartitionTable(tbInfo) {
		return nil
	}
	return errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(partitionName, tbInfo.Name.O))
}

func truncateTTLPartitions(pid int64, pi *model.PartitionInfo) ([]model.PartitionDefinition, []int64) {
	// save current retention partition ID to delete partition data
	oldIDs := []int64{pi.Definitions[ttlRetentionPartitionIndex].ID}
	pi.Definitions[ttlRetentionPartitionIndex].ID = pi.Definitions[ttlCurrentPartitionIndex].ID
	pi.Definitions[ttlCurrentPartitionIndex].ID = pid
	newPartitions := make([]model.PartitionDefinition, 2)
	newPartitions[ttlCurrentPartitionIndex] = pi.Definitions[ttlCurrentPartitionIndex]
	newPartitions[ttlRetentionPartitionIndex] = pi.Definitions[ttlRetentionPartitionIndex]
	return newPartitions, oldIDs
}

func GetCurrentPhysicalTime(storage kv.Storage) time.Time {
	// we know that low resoultion tso will never fail
	ts, _ := storage.GetOracle().GetLowResolutionTimestamp(context.TODO())
	return oracle.GetTimeFromTS(ts)
}

func nextTTLTruncateTime(storage kv.Storage, ttl time.Duration) time.Time {
	return GetCurrentPhysicalTime(storage).Add(ttl)
}

func ToJsonPretty(a interface{}) string {
	b, e := json.MarshalIndent(a, "", "  ")
	if e != nil {
		return e.Error()
	} else {
		return string(b)
	}
}
