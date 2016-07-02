package zfs

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

var (
	TempDir string
	noExDs  *Dataset
	invDs   *Dataset
	noExSn  *Snapshot
)

func TestMain(m *testing.M) {
	exitStatus := 1
	defer func() { os.Exit(exitStatus) }()

	var err error

	TempDir, err = ioutil.TempDir("", "sjlTestZfs")
	if err != nil {
		return
	}
	defer os.RemoveAll(TempDir)

	poolFile, err := ioutil.TempFile(TempDir, "pool")
	if err != nil {
		return
	}
	defer os.Remove(poolFile.Name())

	// Make file 2GB
	if err := poolFile.Truncate(2e9); err != nil {
		return
	}
	poolFile.Close()

	// Create a zfs pool for testing
	poolCreate := exec.Command("zpool",
		"create",
		"sjlTestPool",
		poolFile.Name())
	poolDestroy := exec.Command("zpool",
		"destroy",
		"-f",
		"sjlTestPool")

	err = poolCreate.Run()
	if err != nil {
		e := poolDestroy.Run()
		if e != nil {
			return
		}
		e = poolCreate.Run()
		if e != nil {
			return
		}
	}
	defer poolDestroy.Run()

	noExDs = &Dataset{
		Name: "sjlTestPool/NonExistentDataset",
	}

	invDs = &Dataset{
		Name: "sjlTestPool/invalid@Dataset",
	}

	noExSn = &Snapshot{
		Name: "sjlTestPool/NonExistentDataset@snapshot",
	}

	exitStatus = m.Run()
}

func TestGetDatasetNotExists(t *testing.T) {
	_, err := GetDataset(noExDs.Name)
	assert.Error(t, err, "GetDataset sjlTestPool/TestCreateDataset succeeded when it shouldn't exist")
}

func TestCreateInvalidDataset(t *testing.T) {
	_, err := CreateDataset(invDs.Name, nil)
	assert.Error(t, err, "Create dataset with invalid name succeeded")
}

func TestDestroyNonexistentDataset(t *testing.T) {
	err := noExDs.Destroy()
	assert.Error(t, err, "Destroying nonexistent dataset succeeded")
}

func TestGetNonexistentSnapshot(t *testing.T) {
	_, err := GetSnapshot(noExSn.Name)
	assert.Error(t, err, "Getting nonexistent snapshot succeeded")
}

func TestSnapshotNonexistentDataset(t *testing.T) {
	_, err := noExDs.Snapshot("test")
	assert.Error(t, err, "Snapshotting non existent dataset succeeded")
}

func TestCloneNonexistentSnapshot(t *testing.T) {
	_, err := noExSn.Clone(noExDs.Name)
	assert.Error(t, err, "Cloning non existent dataset succeeded")
}

func TestCreateDataset(t *testing.T) {
	assert := assert.New(t)
	assert.False(DatasetExists("sjlTestPool/TestCreateDataset"),
		"sjlTestPool/TestCreateDataset exists before creating it")

	ds, err := CreateDataset("sjlTestPool/TestCreateDataset", nil)
	assert.NoError(err)

	assert.True(DatasetExists("sjlTestPool/TestCreateDataset"),
		"sjlTestPool/TestCreateDataset dataset does not exist after creating it")

	assert.NoError(ds.Destroy())
}

func TestCreateDatasetMount(t *testing.T) {
	assert := assert.New(t)
	dsOpts := make(map[string]string)
	dsOpts["mountpoint"] = TempDir + "/testdsmount"
	ds, err := CreateDataset("sjlTestPool/TestCreateDatasetMount", dsOpts)
	assert.NoError(err)

	assert.True(DatasetExists("sjlTestPool/TestCreateDatasetMount"),
		"sjlTestPool/TestCreateDatasetMount dataset does not exist after creating it")

	_, err = os.Stat(TempDir + "/testdsmount")
	assert.False(os.IsNotExist(err),
		TempDir+"/testdsmount", " does not exist after creating a dataset mounted there")

	dsNoExist := &Dataset{
		Name: "foobar",
	}
	_, err = dsNoExist.GetMountpoint()
	assert.Error(err, "GetMountPoint for non-existent datastore did not return err")

	mp, err := ds.GetMountpoint()
	assert.NoError(err)
	assert.Equal(mp, TempDir+"/testdsmount", "GetMountPoint not equal to the mountpoint set by CreateDataSet")

	assert.NoError(ds.Destroy())

	empty, err := IsEmpty(TempDir + "/testdsmount")
	assert.NoError(err)

	assert.True(empty, "Newly created dataset is not empty")
}

func TestCloneDataset(t *testing.T) {
	assert := assert.New(t)
	ds, err := CreateDataset("sjlTestPool/TestCloneSrc", nil)
	assert.NoError(err)
	defer func() { assert.NoError(ds.Destroy()) }()

	d1 := []byte("test\nfile\n")
	assert.NoError(ioutil.WriteFile("/sjlTestPool/TestCloneSrc/testfile", d1, 0644))

	sn, err := ds.Snapshot("0")
	assert.NoError(err)

	dsc, err := sn.Clone("sjlTestPool/TestCloneDst")
	assert.NoError(err)
	defer func() { assert.NoError(dsc.Destroy()) }()

	empty, err := IsEmpty("/sjlTestPool/TestCloneDst")
	assert.NoError(err)

	assert.False(empty, "Cloned dataset is empty")
}

func TestPromoteDataset(t *testing.T) {
	assert := assert.New(t)
	ds, err := CreateDataset("sjlTestPool/TestPromoteSrc", nil)
	assert.NoError(err)
	// Don't defer, manually destroy it first, it should work since the destination is promoted
	//defer DestroyDataset("sjlTestPool/TestPromoteSrc")

	d1 := []byte("test\nfile\n")
	assert.NoError(ioutil.WriteFile("/sjlTestPool/TestPromoteSrc/testfile", d1, 0644))

	sn, err := ds.Snapshot("0")
	assert.NoError(err)

	dsc, err := sn.Clone("sjlTestPool/TestPromoteDst")
	assert.NoError(err)
	defer func() { assert.NoError(dsc.Destroy()) }()

	assert.NoError(dsc.Promote())

	empty, err := IsEmpty("/sjlTestPool/TestPromoteDst")
	assert.NoError(err)

	assert.False(empty, "Promoted dataset is empty")

	assert.NoError(ds.Destroy(), "Can't destroy source after dst was promoted.")
}

func TestEmptyNonExist(t *testing.T) {
	_, err := IsEmpty("/tmp/sjlEmptyNotExists")
	assert.Error(t, err, "IsEmpty on non-existent directory does not return err")
}
