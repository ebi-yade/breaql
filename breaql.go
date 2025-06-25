package breaql

import (
	"strings"

	"github.com/samber/lo"
)

type BreakingChanges struct {
	Tables    TableChanges    `json:"tables"`
	Indexes   IndexChanges    `json:"indexes"`
	Schemas   SchemaChanges   `json:"schemas"`
	Databases DatabaseChanges `json:"databases"`
}

func NewBreakingChanges() BreakingChanges {
	return BreakingChanges{
		Tables:    make(TableChanges),
		Indexes:   make(IndexChanges),
		Schemas:   make(SchemaChanges),
		Databases: make(DatabaseChanges),
	}
}

// Exist return if any changes exist.
func (bc BreakingChanges) Exist() bool {
	return bc.Tables.Exist() || bc.Schemas.Exist() || bc.Databases.Exist() || bc.Indexes.Exist()
}

// FormatSQL returns the breaking changes in SQL format.
func (bc BreakingChanges) FormatSQL() string {
	builder := strings.Builder{}
	for _, table := range bc.Tables.Tables() {
		builder.WriteString("-- Table: " + table + "\n")
		for _, stmt := range bc.Tables.Statements(table) {
			builder.WriteString("        " + stmt + "\n")
		}
	}
	for _, index := range bc.Indexes.Indexes() {
		builder.WriteString("-- Index: " + index + "\n")
		for _, stmt := range bc.Indexes.Statements(index) {
			builder.WriteString("        " + stmt + "\n")
		}
	}
	for _, schema := range bc.Schemas.Schemas() {
		builder.WriteString("-- Schema: " + schema + "\n")
		for _, stmt := range bc.Schemas.Statements(schema) {
			builder.WriteString("        " + stmt + "\n")
		}
	}
	for _, database := range bc.Databases.Databases() {
		builder.WriteString("-- Database: " + database + "\n")
		for _, stmt := range bc.Databases.Statements(database) {
			builder.WriteString("        " + stmt + "\n")
		}
	}

	return builder.String()
}

type TableChanges map[string][]string

func (tc TableChanges) add(table string, statements ...string) {
	tc[table] = append(tc[table], statements...)
}

// Tables returns the affected table names.
func (tc TableChanges) Tables() []string {
	return lo.Keys(tc)
}

// Statements returns the breaking statements for the given table.
func (tc TableChanges) Statements(table string) []string {
	return tc[table]
}

// Exist return if any changes exist.
func (tc TableChanges) Exist() bool {
	return len(tc) > 0
}

type IndexChanges map[string][]string

func (ic IndexChanges) add(index string, statements ...string) {
	ic[index] = append(ic[index], statements...)
}

func (ic IndexChanges) Indexes() []string {
	return lo.Keys(ic)
}

// Statements returns the breaking statements for the given index.
func (ic IndexChanges) Statements(index string) []string {
	return ic[index]
}

// Exist return if any changes exist.
func (ic IndexChanges) Exist() bool {
	return len(ic) > 0
}

type SchemaChanges map[string][]string

func (sc SchemaChanges) add(schema string, statements ...string) {
	sc[schema] = append(sc[schema], statements...)
}

// Schemas returns the affected schema names.
func (sc SchemaChanges) Schemas() []string {
	return lo.Keys(sc)
}

// Statements returns the breaking statements for the given schema.
func (sc SchemaChanges) Statements(schema string) []string {
	return sc[schema]
}

// Exist return if any changes exist.
func (sc SchemaChanges) Exist() bool {
	return len(sc) > 0
}

type DatabaseChanges map[string][]string

func (dc DatabaseChanges) add(database string, statements ...string) {
	dc[database] = append(dc[database], statements...)
}

// Databases returns the affected database names.
func (dc DatabaseChanges) Databases() []string {
	return lo.Keys(dc)
}

// Statements returns the breaking statements for the given database.
func (dc DatabaseChanges) Statements(database string) []string {
	return dc[database]
}

// Exist return if any changes exist.
func (dc DatabaseChanges) Exist() bool {
	return len(dc) > 0
}
