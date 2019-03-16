package mysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/Mitu217/tamate"
	"github.com/Mitu217/tamate/driver"
	"github.com/stretchr/testify/assert"
)

const (
	ConnectionTestUser     = "root"
	ConnectionTestPassword = "example"
)

func Test_GetSchema(t *testing.T) {
	var (
		ctx       = context.Background()
		dbName    = "tamatest"
		tableName = "example"
		dsn       = fmt.Sprintf("%s:%s@/%s", ConnectionTestUser, ConnectionTestPassword, dbName)
	)

	// Prepare test
	fakeSchema := &driver.Schema{
		Name: tableName,
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
		Columns: []*driver.Column{
			driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
			driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
		},
	}
	assert.NoError(t, dropDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, createDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, dropTable(ConnectionTestUser, ConnectionTestPassword, dbName, tableName))
	assert.NoError(t, createTable(ConnectionTestUser, ConnectionTestPassword, dbName, fakeSchema))

	// Open datasource
	ds, err := tamate.Open("mysql", dsn)
	defer ds.Close()
	assert.NoError(t, err)

	// Getting schema
	sc, err := ds.GetSchema(ctx, tableName)
	if assert.NoError(t, err) {
		assert.Equal(t, fakeSchema.Name, sc.Name)
		assert.Equal(t, fakeSchema.PrimaryKey, sc.PrimaryKey)
		assert.Equal(t, fakeSchema.Columns, sc.Columns)
	}
}

func Test_SetSchema(t *testing.T) {
	var (
		ctx       = context.Background()
		dbName    = "tamatest"
		tableName = "example"
		dsn       = fmt.Sprintf("%s:%s@/%s", ConnectionTestUser, ConnectionTestPassword, dbName)
	)

	// Prepare test
	fakeSchema := &driver.Schema{
		Name: tableName,
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
		Columns: []*driver.Column{
			driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
			driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
		},
	}
	assert.NoError(t, dropDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, createDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))

	// Open datasource
	ds, err := tamate.Open("mysql", dsn)
	defer ds.Close()
	assert.NoError(t, err)

	// Setting schema
	assert.NoError(t, ds.SetSchema(ctx, tableName, fakeSchema))
	rows, err := getInformationSchema(ConnectionTestUser, ConnectionTestPassword, dbName, tableName)
	if assert.NoError(t, err) {
		for rows.Next() {
			var columnName string
			var ordinalPosition int
			var columnType string
			var columnKey string
			var isNullable string
			var extra string
			assert.NoError(t, rows.Scan(&columnName, &ordinalPosition, &columnType, &columnKey, &isNullable, &extra))
			switch columnName {
			case "id":
				assert.Equal(t, 1, ordinalPosition)
				assert.Equal(t, "int(11)", columnType)
				assert.Equal(t, "PRI", columnKey)
				assert.Equal(t, "NO", isNullable)
				assert.Equal(t, "", extra)
			case "name":
				assert.Equal(t, 2, ordinalPosition)
				assert.Equal(t, "text", columnType)
				assert.Equal(t, "", columnKey)
				assert.Equal(t, "NO", isNullable)
				assert.Equal(t, "", extra)
			default:
				t.Errorf("undefined column: %s", columnName)
			}
		}
	}
}

func Test_GetRows(t *testing.T) {
	var (
		ctx       = context.Background()
		dbName    = "tamatest"
		tableName = "example"
		dsn       = fmt.Sprintf("%s:%s@/%s", ConnectionTestUser, ConnectionTestPassword, dbName)
	)

	// Prepare test
	fakeSchema := &driver.Schema{
		Name: tableName,
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
		Columns: []*driver.Column{
			driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
			driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
		},
	}
	fakeRows := []*driver.Row{
		&driver.Row{
			Values: map[string]*driver.GenericColumnValue{
				"id": &driver.GenericColumnValue{
					Column: driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
					Value:  1,
				},
				"name": &driver.GenericColumnValue{
					Column: driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
					Value:  "user",
				},
			},
		},
	}
	assert.NoError(t, dropDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, createDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, dropTable(ConnectionTestUser, ConnectionTestPassword, dbName, tableName))
	assert.NoError(t, createTable(ConnectionTestUser, ConnectionTestPassword, dbName, fakeSchema))
	for _, fakeRow := range fakeRows {
		_, err := insertRow(ConnectionTestUser, ConnectionTestPassword, dbName, tableName, fakeRow)
		assert.NoError(t, err)
	}

	// Open datasource
	ds, err := tamate.Open("mysql", dsn)
	defer ds.Close()
	assert.NoError(t, err)

	// Getting rows
	rows, err := ds.GetRows(ctx, tableName)
	if assert.NoError(t, err) {
		for _, row := range rows {
			for key := range row.Values {
				switch key {
				case "id":
					assert.Equal(t, int64(1), row.Values[key].Value)
				case "name":
					assert.Equal(t, "user", row.Values[key].Value)
				default:
					t.Errorf("undefined column: %s", key)
				}
			}
		}
	}
}

func Test_SetRows(t *testing.T) {
	var (
		ctx       = context.Background()
		dbName    = "tamatest"
		tableName = "example"
		dsn       = fmt.Sprintf("%s:%s@/%s", ConnectionTestUser, ConnectionTestPassword, dbName)
	)

	// Prepare test
	fakeSchema := &driver.Schema{
		Name: tableName,
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
		Columns: []*driver.Column{
			driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
			driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
		},
	}
	fakeRows := []*driver.Row{
		&driver.Row{
			Values: map[string]*driver.GenericColumnValue{
				"id": &driver.GenericColumnValue{
					Column: driver.NewColumn("id", 0, driver.ColumnTypeInt, true, false),
					Value:  1,
				},
				"name": &driver.GenericColumnValue{
					Column: driver.NewColumn("name", 1, driver.ColumnTypeString, true, false),
					Value:  "user",
				},
			},
		},
	}
	assert.NoError(t, dropDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, createDatabase(ConnectionTestUser, ConnectionTestPassword, dbName))
	assert.NoError(t, dropTable(ConnectionTestUser, ConnectionTestPassword, dbName, tableName))
	assert.NoError(t, createTable(ConnectionTestUser, ConnectionTestPassword, dbName, fakeSchema))

	// Open datasource
	ds, err := tamate.Open("mysql", dsn)
	defer ds.Close()
	assert.NoError(t, err)

	// Setting rows
	if assert.NoError(t, ds.SetRows(ctx, tableName, fakeRows)) {
		rows, err := selectRows(ConnectionTestUser, ConnectionTestPassword, dbName, tableName)
		assert.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var id int64
			var name string
			assert.NoError(t, rows.Scan(&id, &name))
			assert.Equal(t, int64(1), id)
			assert.Equal(t, "user", name)
		}
	}
}
