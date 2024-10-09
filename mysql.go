package breaql

import (
	"log/slog"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/samber/lo"

	// Importing the following parser driver causes a build error.
	//_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// RunMySQL parses the given (possibly composite) DDL statements and returns the breaking ones.
func RunMySQL(sql string) (BreakingChanges, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return BreakingChanges{}, &ParseError{original: err, Message: err.Error(), funcName: "parser.Parse"}
	}

	changes := NewBreakingChanges()

	for _, stmtNode := range stmtNodes {
		stmtText := strings.TrimSpace(stmtNode.Text())
		slog.Debug("processing stmt", slog.String("stmt", stmtText))

		switch stmt := stmtNode.(type) {
		case *ast.DropDatabaseStmt:
			changes.Databases.add(stmt.Name.String(), stmtText)

		case *ast.DropTableStmt:
			lo.ForEach(stmt.Tables, func(stmt *ast.TableName, _ int) { changes.Tables.add(stmt.Name.String(), stmtText) })

		case *ast.TruncateTableStmt:
			changes.Tables.add(stmt.Table.Name.String(), stmtText)

		case *ast.RenameTableStmt:
			lo.ForEach(stmt.TableToTables, func(ttt *ast.TableToTable, _ int) { changes.Tables.add(ttt.OldTable.Name.String(), stmtText) })

		case *ast.AlterTableStmt:
			for _, spec := range stmt.Specs {
				if isBreakingAlterTableSpec(spec) {
					changes.Tables.add(stmt.Table.Name.String(), stmtText)
					break
				}
			}
		}

	}

	return changes, nil
}

func isBreakingAlterTableSpec(spec *ast.AlterTableSpec) bool {
	switch spec.Tp {
	case ast.AlterTableDropColumn, ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey, ast.AlterTableDropPrimaryKey:
		return true

	case ast.AlterTableModifyColumn:
		// Note: False positives are accepted here as we cannot obtain the old column type.
		return true

	default:
		return false
	}
}
