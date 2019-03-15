package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Mitu217/tamate/driver"
)

//--------------------
// Convert ColumnType
//--------------------

func columnTypeFromMySQLToGeneric(ct string) (driver.ColumnType, error) {
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

func columnTypeFromGenericToMySQL(ct driver.ColumnType) (string, error) {
	switch ct {
	case driver.ColumnTypeInt:
		return "INT", nil
	case driver.ColumnTypeFloat:
		return "FLOAT", nil
	case driver.ColumnTypeBool:
		return "BOOLEAN", nil
	case driver.ColumnTypeDatetime:
		return "DATETIME", nil
	case driver.ColumnTypeDate:
		return "DATE", nil
	case driver.ColumnTypeString:
		return "TEXT", nil
	case driver.ColumnTypeBytes:
		return "BLOB", nil
	default:
		return "", fmt.Errorf("conversion not found for Generic type: %s", ct)
	}
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
		return reflect.TypeOf(nil)
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

//------------
// Exec Query
//------------

func exec(user, password, dbName, query string) (sql.Result, error) {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.Exec(query)
}

func dropDatabase(user, password, dbName string) error {
	q, err := generateDropDBQuery(dbName)
	if err != nil {
		return err
	}
	if _, err := exec(user, password, "", q); err != nil {
		return err
	}
	return nil
}

func createDatabase(user, password, dbName string) error {
	q, err := generateCreateDBQuery(dbName)
	if err != nil {
		return err
	}
	if _, err := exec(user, password, "", q); err != nil {
		return err
	}
	return nil
}

func createTable(user, password, dbName string, sc *driver.Schema) error {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return createTableDB(db, sc)
}

func createTableDB(db *sql.DB, sc *driver.Schema) error {
	q, err := generateCreateTableQuery(sc)
	if err != nil {
		return err
	}
	if _, err := db.Exec(q); err != nil {
		return err
	}
	return nil
}

func dropTable(user, password, dbName, tableName string) error {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return dropTableDB(db, tableName)
}

func dropTableDB(db *sql.DB, tableName string) error {
	q, err := generateDropTableQuery(tableName)
	if err != nil {
		return err
	}
	if _, err := db.Exec(q); err != nil {
		return err
	}
	return nil
}

func getInformationSchema(user, password, dbName, tableName string) (*sql.Rows, error) {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return getInfomationSchemaDB(db, tableName)
}

func getInfomationSchemaDB(db *sql.DB, tableName string) (*sql.Rows, error) {
	q, err := generateGetInformationSchemaQuery(tableName)
	if err != nil {
		return nil, err
	}
	return db.Query(q)
}

func selectRows(user, password, dbName, tableName string) (*sql.Rows, error) {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return selectRowsDB(db, tableName)
}

func selectRowsDB(db *sql.DB, tableName string) (*sql.Rows, error) {
	q, err := generateSelectRowsQuery(tableName)
	if err != nil {
		return nil, err
	}
	return db.Query(q)
}

func insertRow(user, password, dbName, tableName string, row *driver.Row) (sql.Result, error) {
	dsn := fmt.Sprintf("%s:%s@/%s", user, password, dbName)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return insertRowDB(db, tableName, row)
}

func insertRowDB(db *sql.DB, tableName string, row *driver.Row) (sql.Result, error) {
	q, err := generateInsertRowQuery(tableName, row)
	if err != nil {
		return nil, err
	}
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	values := make([]interface{}, len(row.Values))
	for _, rowVal := range row.Values {
		values[rowVal.Column.OrdinalPosition] = rowVal.Value
	}
	return stmt.Exec(values...)
}
