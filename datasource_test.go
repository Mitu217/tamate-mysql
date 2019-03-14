package mysql

import (
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlTestDataRowCount = 100
)

/*
func TestMySQLDatasource_Get(t *testing.T) {
	dsn := os.Getenv("TAMATE_MYSQL_DSN")
	if dsn == "" {
		t.Skip("env: TAMATE_MYSQL_DSN not set")
	}

	before(dsn)

	ds, err := NewMySQLDatasource(dsn + "tamatest")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if err := ds.Open(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sc, err := ds.GetSchema(ctx, "example")
	if err != nil {
		t.Fatal(err)
	}

	if len(sc.PrimaryKey.ColumnNames) != 1 || sc.PrimaryKey.ColumnNames[0] != "id" {
		t.Fatal("PK must be [id]")
	}

	if len(sc.Columns) != 2 || sc.Columns[1].Name != "name" || sc.Columns[1].Type != ColumnTypeString {
		t.Fatal("Columns[1] must be string-type 'name'")
	}

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatalf("GetRows failed: %+v", err)
	}

	if len(rows) != mysqlTestDataRowCount {
		t.Fatalf("len(rows.Value) must be %d", mysqlTestDataRowCount)
	}

	for i := 0; i < mysqlTestDataRowCount; i++ {
		if rows[i].Values["id"].Value != int64(i) {
			t.Fatalf("rows[%d].Values['id'] must be %d, but actual: %+v (type: %+v)", i, i, rows[i].Values["id"].Value, reflect.TypeOf(rows[i].Values["id"].Value))
		}
		if rows[i].Values["name"].Value != fmt.Sprintf("name%d", i) {
			t.Fatalf("rows[%d].Values['name'] must be 'name%d'", i, i)
		}
	}

	after(dsn)
}

func before(dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP DATABASE IF EXISTS `tamatest`"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE `tamatest`"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("USE `tamatest`"); err != nil {
		log.Fatal(err)
	}
	if err := insertTestData(db); err != nil {
		log.Fatal(err)
	}
}

func insertTestData(db *sql.DB) error {
	if _, err := db.Exec("drop table if exists `example`"); err != nil {
		return err
	}
	if _, err := db.Exec("create table `example` (`id` int not null primary key, `name` varchar(100) not null)"); err != nil {
		return err
	}
	for i := 0; i < mysqlTestDataRowCount; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO `example` VALUES(%d, 'name%d')", i, i)); err != nil {
			return err
		}
	}
	return nil
}

func after(dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP DATABASE IF EXISTS `tamatest`"); err != nil {
		log.Fatal(err)
	}
}
*/
