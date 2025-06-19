package breaql

import (
	"log/slog"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// RunPostgreSQL parses the given DDL statements and returns the breaking ones.
func RunPostgreSQL(sql string) (BreakingChanges, error) {
	sql = strings.TrimSpace(sql)
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}

	tree, err := pg_query.Parse(sql)
	if err != nil {
		return BreakingChanges{}, &ParseError{original: err, Message: err.Error(), funcName: "pg_query.Parse"}
	}

	changes := NewBreakingChanges()

	for _, rawStmt := range tree.Stmts {
		start := rawStmt.StmtLocation
		end := start + rawStmt.StmtLen
		stmtText := strings.TrimSpace(sql[start:end]) + ";"
		slog.Info("processing stmt", slog.String("stmt", stmtText))

		node := rawStmt.Stmt
		switch n := node.Node.(type) {
		case *pg_query.Node_DropdbStmt:
			changes.Databases.add(n.DropdbStmt.Dbname, stmtText)

		case *pg_query.Node_DropStmt:
			switch n.DropStmt.RemoveType {
			case pg_query.ObjectType_OBJECT_TABLE:
				for _, obj := range n.DropStmt.Objects {
					if list, ok := obj.Node.(*pg_query.Node_List); ok && len(list.List.Items) > 0 {
						var parts []string
						for _, item := range list.List.Items {
							if str, ok := item.Node.(*pg_query.Node_String_); ok {
								parts = append(parts, str.String_.GetSval())
							}
						}
						table := strings.Join(parts, ".")
						changes.Tables.add(table, stmtText)
					}
				}

			case pg_query.ObjectType_OBJECT_INDEX:
				for _, obj := range n.DropStmt.Objects {
					if list, ok := obj.Node.(*pg_query.Node_List); ok && len(list.List.Items) > 0 {
						var parts []string
						for _, item := range list.List.Items {
							if str, ok := item.Node.(*pg_query.Node_String_); ok {
								parts = append(parts, str.String_.GetSval())
							}
						}
						index := strings.Join(parts, ".")
						changes.Indexes.add(index, stmtText)
					}
				}
			}

		case *pg_query.Node_TruncateStmt:
			for _, rel := range n.TruncateStmt.Relations {
				if rv, ok := rel.Node.(*pg_query.Node_RangeVar); ok {
					changes.Tables.add(rv.RangeVar.Relname, stmtText)
				}
			}

		case *pg_query.Node_RenameStmt:
			if n.RenameStmt.Relation != nil {
				changes.Tables.add(n.RenameStmt.Relation.Relname, stmtText)
			}

		case *pg_query.Node_AlterTableStmt:
			if n.AlterTableStmt.Relation != nil {
				if slices.ContainsFunc(n.AlterTableStmt.Cmds, isBreakingAlterTableCmd) {
					changes.Tables.add(n.AlterTableStmt.Relation.Relname, stmtText)
				}
			}
		}
	}

	return changes, nil
}

func isBreakingAlterTableCmd(cmd *pg_query.Node) bool {
	switch c := cmd.Node.(type) {
	case *pg_query.Node_AlterTableCmd:
		switch c.AlterTableCmd.Subtype {
		case pg_query.AlterTableType_AT_DropColumn,
			pg_query.AlterTableType_AT_DropConstraint,
			pg_query.AlterTableType_AT_AlterColumnType:
			return true
		}
	}
	return false
}
