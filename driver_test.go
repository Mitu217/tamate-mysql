package mysql

import (
	"fmt"
	"testing"

	"github.com/go-tamate/tamate"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
)

const (
	DriverTestUser     = "root"
	DriverTestPassword = "example"
	DriverTestDBName   = "tamatest"
)

func Test_Init(t *testing.T) {
	drivers := tamate.Drivers()
	d, has := drivers[driverName]
	assert.EqualValues(t, &mysqlDriver{}, d)
	assert.True(t, has)
}

func Test_Open(t *testing.T) {
	// before
	assert.NoError(t, dropDatabase(DriverTestUser, DriverTestPassword, DriverTestDBName))
	assert.NoError(t, createDatabase(DriverTestUser, DriverTestPassword, DriverTestDBName))

	dsn := fmt.Sprintf("%s:%s@/%s", DriverTestUser, DriverTestPassword, DriverTestDBName)
	ds, err := tamate.Open(driverName, dsn)
	defer func() {
		err := ds.Close()
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
}
