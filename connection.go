package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Mitu217/tamate/driver"
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

func (c *mysqlConn) GetSchema(ctx context.Context, name string) (*driver.Schema, error) {
	schemaMap, err := c.getSchemaMap()
	if err != nil {
		return nil, err
	}
	for scName, sc := range schemaMap {
		if scName == name {
			return sc, nil
		}
	}
	return nil, errors.New("schema not found: " + name)
}

func (c *mysqlConn) SetSchema(ctx context.Context, name string, schema *driver.Schema) error {
	return fmt.Errorf("feature support")
}

func (c *mysqlConn) GetRows(ctx context.Context, name string) ([]*driver.Row, error) {
	result, err := c.db.Query(fmt.Sprintf("SELECT * FROM %s", name))
	if err != nil {
		return nil, err
	}
	defer result.Close()

	/*
		var rows []*driver.Row
		for result.Next() {
			rowValues := make(driver.RowValues, len(schema.Columns))
			rowValuesGroupByKey := make(driver.GroupByKey)
			ptrs := make([]interface{}, len(schema.Columns))
			for i, col := range schema.Columns {
				ptr := reflect.New(colToMySQLType(col)).Interface()
				ptrs[i] = ptr
			}
			if err := result.Scan(ptrs...); err != nil {
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
	*/
	return nil, nil
}

func (c *mysqlConn) SetRows(ctx context.Context, name string, rows []*driver.Row) error {
	return fmt.Errorf("feature support")
}

func (c *mysqlConn) getSchemaMap() (map[string]*driver.Schema, error) {
	// get schemas
	sqlRows, err := c.db.Query("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, COLUMN_KEY, IS_NULLABLE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()")
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	// scan results
	schemaMap := make(map[string]*driver.Schema)
	for sqlRows.Next() {
		var tableName string
		var columnName string
		var ordinalPosition int
		var columnType string
		var columnKey string
		var isNullable string
		var extra string
		if err := sqlRows.Scan(&tableName, &columnName, &ordinalPosition, &columnType, &columnKey, &isNullable, &extra); err != nil {
			return nil, err
		}
		// prepare schema
		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = &driver.Schema{Name: tableName}
		}
		schema := schemaMap[tableName]
		// set column in schema
		if strings.Contains(columnKey, "PRI") {
			if schema.PrimaryKey == nil {
				schema.PrimaryKey = &driver.Key{
					KeyType: driver.KeyTypePrimary,
				}
			}
			schema.PrimaryKey.ColumnNames = append(schema.PrimaryKey.ColumnNames, columnName)
		}
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
		schemaMap[tableName] = schema
	}

	return schemaMap, nil
}
