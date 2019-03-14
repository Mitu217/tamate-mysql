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
	"github.com/go-sql-driver/mysql"
)

type mysqlConn struct {
	DSN string
	db  *sql.DB
}

func newMySQLConn(dsn string) (*mysqlConn, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &mysqlConn{
		DSN: dsn,
		db:  db,
	}, nil
}

func (c *mysqlConn) Close() error {
	return c.db.Close()
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
		valueType, err := mysqlColumnTypeToValueType(columnType)
		if err != nil {
			return nil, err
		}
		column := &driver.Column{
			Name:            columnName,
			OrdinalPosition: ordinalPosition - 1,
			Type:            valueType,
			NotNull:         isNullable != "YES",
			AutoIncrement:   strings.Contains(extra, "auto_increment"),
		}
		schema.Columns = append(schema.Columns, column)
		schemaMap[tableName] = schema
	}

	return schemaMap, nil
}

func mysqlColumnTypeToValueType(ct string) (driver.ColumnType, error) {
	ct = strings.ToLower(ct)
	if strings.HasPrefix(ct, "int") ||
		strings.HasPrefix(ct, "smallint") ||
		strings.HasPrefix(ct, "mediumint") ||
		strings.HasPrefix(ct, "bigint") {
		return driver.ColumnTypeInt, nil
	}
	if strings.HasPrefix(ct, "float") ||
		strings.HasPrefix(ct, "double") ||
		strings.HasPrefix(ct, "decimal") {
		return driver.ColumnTypeFloat, nil
	}
	if strings.HasPrefix(ct, "char") ||
		strings.HasPrefix(ct, "varchar") ||
		strings.HasPrefix(ct, "text") ||
		strings.HasPrefix(ct, "mediumtext") ||
		strings.HasPrefix(ct, "longtext") ||
		strings.HasPrefix(ct, "json") {
		return driver.ColumnTypeString, nil
	}
	if strings.HasPrefix(ct, "datetime") ||
		strings.HasPrefix(ct, "timestamp") {
		return driver.ColumnTypeDatetime, nil
	}
	if strings.HasPrefix(ct, "date") {
		return driver.ColumnTypeDate, nil
	}
	if strings.HasPrefix(ct, "blob") {
		return driver.ColumnTypeBytes, nil
	}
	return driver.ColumnTypeNull, fmt.Errorf("conversion not found for MySQL type: %s", ct)
}

func colToMySQLType(c *driver.Column) reflect.Type {
	switch c.Type {
	case driver.ColumnTypeInt:
		if c.NotNull {
			return reflect.TypeOf(int64(0))
		}
		return reflect.TypeOf(sql.NullInt64{})

	case driver.ColumnTypeFloat:
		if c.NotNull {
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(sql.NullFloat64{})
	case driver.ColumnTypeBool:
		if c.NotNull {
			return reflect.TypeOf(false)
		}
		return reflect.TypeOf(sql.NullBool{})
	case driver.ColumnTypeDatetime, driver.ColumnTypeDate:
		if c.NotNull {
			return reflect.TypeOf(time.Time{})
		}
		return reflect.TypeOf(mysql.NullTime{})
	case driver.ColumnTypeString:
		if c.NotNull {
			return reflect.TypeOf("")
		}
		return reflect.TypeOf(sql.NullString{})
	case driver.ColumnTypeBytes:
		return reflect.TypeOf([]byte{})
	}
	return reflect.TypeOf(nil)
}
