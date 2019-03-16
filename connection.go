package mysql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/go-tamate/tamate/driver"
)

type mysqlConn struct {
	DSN string
	db  *sql.DB
}

func newMySQLConn(dsn string) (*mysqlConn, error) {
	mc := &mysqlConn{
		DSN: dsn,
	}
	if err := mc.Open(); err != nil {
		return nil, err
	}
	return mc, nil
}

func (c *mysqlConn) Open() error {
	db, err := sql.Open("mysql", c.DSN)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *mysqlConn) Close() error {
	if c.db == nil {
		return errors.New("datastore is not opened")
	}
	if err := c.db.Close(); err != nil {
		return err
	}
	c.db = nil
	return nil
}

func (c *mysqlConn) GetSchema(ctx context.Context, tableName string) (*driver.Schema, error) {
	rows, err := getInfomationSchemaDB(c.db, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schema *driver.Schema
	for rows.Next() {
		if schema == nil {
			schema = &driver.Schema{Name: tableName}
		}

		var columnName string
		var ordinalPosition int
		var columnType string
		var columnKey string
		var isNullable string
		var extra string
		if err := rows.Scan(&columnName, &ordinalPosition, &columnType, &columnKey, &isNullable, &extra); err != nil {
			return nil, err
		}

		// key
		if strings.Contains(columnKey, "PRI") {
			if schema.PrimaryKey == nil {
				schema.PrimaryKey = &driver.Key{
					KeyType: driver.KeyTypePrimary,
				}
			}
			schema.PrimaryKey.ColumnNames = append(schema.PrimaryKey.ColumnNames, columnName)
		}

		// column
		ct, err := columnTypeFromMySQLToGeneric(columnType)
		if err != nil {
			return nil, err
		}
		column := &driver.Column{
			Name:            columnName,
			OrdinalPosition: ordinalPosition - 1,
			Type:            ct,
			NotNull:         isNullable != "YES",
			AutoIncrement:   strings.Contains(extra, "auto_increment"),
		}
		schema.Columns = append(schema.Columns, column)
	}

	if schema == nil {
		return nil, errors.New("schema not found: " + tableName)
	}
	return schema, nil
}

func (c *mysqlConn) SetSchema(ctx context.Context, tableName string, sc *driver.Schema) error {
	dropTableDB(c.db, tableName)
	createTableDB(c.db, sc)
	return nil
}

func (c *mysqlConn) GetRows(ctx context.Context, tableName string) ([]*driver.Row, error) {
	schema, err := c.GetSchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	resultRows, err := selectRowsDB(c.db, tableName)
	if err != nil {
		return nil, err
	}
	defer resultRows.Close()

	var rows []*driver.Row
	for resultRows.Next() {
		rowValues := make(driver.RowValues, len(schema.Columns))
		rowValuesGroupByKey := make(driver.GroupByKey)
		ptrs := make([]interface{}, len(schema.Columns))
		for i, col := range schema.Columns {
			ptr := reflect.New(colToMySQLType(col)).Interface()
			ptrs[i] = ptr
		}
		if err := resultRows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, col := range schema.Columns {
			val := reflect.ValueOf(ptrs[i]).Elem().Interface()
			colValue := &driver.GenericColumnValue{Column: col, Value: val}
			rowValues[col.Name] = colValue
			for i := range schema.PrimaryKey.ColumnNames {
				if schema.PrimaryKey.ColumnNames[i] == col.Name {
					key := schema.PrimaryKey.String()
					rowValuesGroupByKey[key] = append(rowValuesGroupByKey[key], colValue)
				}
			}
		}
		rows = append(rows, &driver.Row{GroupByKey: rowValuesGroupByKey, Values: rowValues})
	}
	return rows, nil
}

func (c *mysqlConn) SetRows(ctx context.Context, tableName string, rows []*driver.Row) error {
	sc, err := c.GetSchema(ctx, tableName)
	if err != nil {
		return err
	}

	dropTableDB(c.db, tableName)
	createTableDB(c.db, sc)

	for _, row := range rows {
		_, err := insertRowDB(c.db, tableName, row)
		if err != nil {
			return err
		}
	}

	return nil
}
