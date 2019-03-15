package mysql

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/Mitu217/tamate"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
)

const (
	MYSQL_USER     = "root"
	MYSQL_PASSWORD = "example"
	DB_NAME        = "tamatest"
)

func beforeAll() error {
	if err := dropDatabase(fmt.Sprintf("%s:%s@/", MYSQL_USER, MYSQL_PASSWORD), DB_NAME); err != nil {
		return err
	}
	if err := createDatabase(fmt.Sprintf("%s:%s@/", MYSQL_USER, MYSQL_PASSWORD), DB_NAME); err != nil {
		return err
	}
	return nil
}

func afterAll() error {
	return nil
}

func TestMain(m *testing.M) {
	if err := beforeAll(); err != nil {
		os.Exit(-1)
	}
	code := m.Run()
	if err := afterAll(); err != nil {
		os.Exit(-1)
	}
	os.Exit(code)
}

func Test_Init(t *testing.T) {
	drivers := tamate.Drivers()
	d, has := drivers[driverName]
	assert.Equal(t, reflect.TypeOf(&mysqlDriver{}), reflect.TypeOf(d))
	assert.True(t, has)
}

func Test_Open(t *testing.T) {
	ds, err := tamate.Open(driverName, fmt.Sprintf("%s:%s@/%s", MYSQL_USER, MYSQL_PASSWORD, DB_NAME))
	defer func() {
		err := ds.Close()
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
}
