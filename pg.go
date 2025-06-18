package breaql

import (
	"log/slog"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// RunPostgreSQL parses the given DDL statements and returns the breaking ones.
func RunPostgreSQL(sql string) (BreakingChanges, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return BreakingChanges{}, &ParseError{original: err, Message: err.Error(), funcName: "pg_query.Parse"}
	}

	changes := NewBreakingChanges()

	for _, rawStmt := range tree.Stmts {
		start := rawStmt.StmtLocation
		end := rawStmt.StmtLocation + rawStmt.StmtLen
		if start < 0 || end > int32(len(sql)) || start > end {
			continue
		}
		stmtText := strings.TrimSpace(sql[start:end])
		if stmtText == "" {
			continue
		}
		stmtText += ";"
		slog.Debug("processing stmt", slog.String("stmt", stmtText))

		node := rawStmt.Stmt
		switch n := node.Node.(type) {
		case *pg_query.Node_DropdbStmt:
			changes.Databases.add(n.DropdbStmt.Dbname, stmtText)

		case *pg_query.Node_DropStmt:
			switch n.DropStmt.RemoveType {
			case pg_query.ObjectType_OBJECT_TABLE:
				for _, obj := range n.DropStmt.Objects {
					if list, ok := obj.Node.(*pg_query.Node_List); ok && len(list.List.Items) > 0 {
						if str, ok := list.List.Items[0].Node.(*pg_query.Node_String_); ok {
							changes.Tables.add(str.String_.GetSval(), stmtText)
						}
					}
				}

			case pg_query.ObjectType_OBJECT_INDEX:
				for _, obj := range n.DropStmt.Objects {
					if list, ok := obj.Node.(*pg_query.Node_List); ok && len(list.List.Items) > 0 {
						if str, ok := list.List.Items[0].Node.(*pg_query.Node_String_); ok {
							changes.Indexes.add(str.String_.GetSval(), stmtText)
						}
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
