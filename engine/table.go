package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

// Table is defined by a name and attributes
// A table with data is called a Relation
type Table struct {
	name       string
	alias      string
	attributes []Attribute
}

// NewTable initializes a new Table
func NewTable(name string) *Table {
	t := &Table{
		name: name,
	}

	return t
}

// NewTableWithAlias initializes a new Table with given alias.
func NewTableWithAlias(name string, alias string) *Table {
	t := &Table{
		name:  name,
		alias: alias,
	}

	return t
}

func NewTableFromDecl(t *parser.Decl) *Table {
	if len(t.Decl) > 0 && t.Decl[0].Token == parser.AsToken {
		return NewTableWithAlias(t.Lexeme, t.Decl[0].Decl[0].Lexeme)
	} else {
		return NewTable(t.Lexeme)
	}
}

// AddAttribute is used by CREATE TABLE and ALTER TABLE
// Want to check that name isn't already taken
func (t *Table) AddAttribute(attr Attribute) error {
	t.attributes = append(t.attributes, attr)
	return nil
}

func (t *Table) Alias() string {
	if t.alias == "" {
		return t.name
	}
	return t.alias
}

// String returns a printable string with table name and attributes
func (t Table) String() string {
	stringy := t.name + " ("
	for i, a := range t.attributes {
		if i != 0 {
			stringy += " | "
		}
		stringy += a.name + " " + a.typeName
	}
	stringy += ")"

	return stringy
}

type Tables []*Table

func (t Tables) ByAlias(alias string) (*Table, bool) {
	for _, tt := range t {
		if tt.Alias() == alias {
			return tt, true
		}
	}
	return nil, false
}

func createTableExecutor(e *Engine, tableDecl *parser.Decl, conn protocol.EngineConn) error {
	var i int

	if len(tableDecl.Decl) == 0 {
		return fmt.Errorf("parsing failed, malformed query")
	}

	// Fetch constrainit (i.e: "IF EXISTS")
	i = 0
	for i < len(tableDecl.Decl) {

		if e.opsExecutors[tableDecl.Decl[i].Token] != nil {
			if err := e.opsExecutors[tableDecl.Decl[i].Token](e, tableDecl.Decl[i], conn); err != nil {
				return err
			}
		} else {
			break
		}

		i++
	}

	// Check if table does not exists
	r := e.relation(tableDecl.Decl[i].Lexeme)
	if r != nil {
		return fmt.Errorf("table %s already exists", tableDecl.Decl[i].Lexeme)
	}

	// Fetch table name
	t := NewTable(tableDecl.Decl[i].Lexeme)

	// Fetch attributes
	i++
	for i < len(tableDecl.Decl) {
		attr, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return err
		}
		err = t.AddAttribute(attr)
		if err != nil {
			return err
		}

		i++
	}

	e.relations[t.name] = NewRelation(t)
	conn.WriteResult(0, 1)
	return nil
}

func tableAlias(decl *parser.Decl) string {
	for _, decl := range decl.Decl {
		if decl.Token == parser.AsToken {
			return decl.Decl[0].Lexeme
		}
	}
	return ""
}
