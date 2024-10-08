package breaql

import (
	"log/slog"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	// Importing the following parser driver causes a build error.
	//_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// RunMySQL parses the given (possibly composite) DDL statements and returns the breaking ones.
func RunMySQL(sql string) ([]string, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "error p.Parse")
	}

	var breakingStmt []string

	for _, stmtNode := range stmtNodes {
		stmtText := strings.TrimSpace(stmtNode.Text())
		slog.Debug("processing stmt", slog.String("stmt", stmtText))

		switch stmt := stmtNode.(type) {
		case *ast.DropTableStmt, *ast.TruncateTableStmt, *ast.DropDatabaseStmt, *ast.RenameTableStmt:
			breakingStmt = append(breakingStmt, stmtText)
		case *ast.AlterTableStmt:
			for _, spec := range stmt.Specs {
				if isBreakingAlterTableSpec(spec) {
					breakingStmt = append(breakingStmt, stmtText)
					break
				}
			}
		}
	}

	return breakingStmt, nil
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
