package zfs

import (
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

	TempDir, err = ioutil.TempDir("", "sjlTest")
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
	if _, err := GetDataset(noExDs.Name); err == nil {
		t.Error("GetDataset sjlTestPool/TestCreateDataset succeeded when it shouldn't exist")
	}
}

func TestCreateInvalidDataset(t *testing.T) {
	if _, err := CreateDataset(invDs.Name, nil); err == nil {
		t.Error("Create dataset with invalid name succeeded")
	}
}

func TestDestroyNonexistentDataset(t *testing.T) {
	if err := noExDs.Destroy(); err == nil {
		t.Error("Destroying nonexistent dataset succeeded")
	}
}

func TestGetNonexistentSnapshot(t *testing.T) {
	if _, err := GetSnapshot(noExSn.Name); err == nil {
		t.Error("Getting nonexistent snapshot succeeded")
	}
}

func TestSnapshotNonexistentDataset(t *testing.T) {
	if _, err := noExDs.Snapshot("test"); err == nil {
		t.Error("Snapshotting non existent dataset succeeded")
	}
}

func TestCloneNonexistentSnapshot(t *testing.T) {
	if _, err := noExSn.Clone(noExDs.Name); err == nil {
		t.Error("Cloning non existent dataset succeeded")
	}
}

func TestCreateDataset(t *testing.T) {
	if DatasetExists("sjlTestPool/TestCreateDataset") {
		t.Error("sjlTestPool/TestCreateDataset exists before creating it")
	}

	ds, err := CreateDataset("sjlTestPool/TestCreateDataset", nil)
	if err != nil {
		t.Error(err)
	}

	if !DatasetExists("sjlTestPool/TestCreateDataset") {
		t.Error("sjlTestPool/TestCreateDataset dataset does not exist after creating it")
	}

	if err := ds.Destroy(); err != nil {
		t.Error(err)
	}

}

func TestCreateDatasetMount(t *testing.T) {
	dsOpts := make(map[string]string)
	dsOpts["mountpoint"] = TempDir + "/testdsmount"
	ds, err := CreateDataset("sjlTestPool/TestCreateDatasetMount", dsOpts)
	if err != nil {
		t.Error(err)
	}

	if !DatasetExists("sjlTestPool/TestCreateDatasetMount") {
		t.Error("sjlTestPool/TestCreateDatasetMount dataset does not exist after creating it")
	}

	if _, err := os.Stat(TempDir + "/testdsmount"); os.IsNotExist(err) {
		t.Error(TempDir+"/testdsmount", " does not exist after creating a dataset mounted there")
	}

	dsNoExist := &Dataset{
		Name: "foobar",
	}
	_, err = dsNoExist.GetMountpoint()
	if err == nil {
		t.Error("GetMountPoint for non-existent datastore did not return err")
	}

	mp, err := ds.GetMountpoint()
	if err != nil {
		t.Error(err)
	}
	if mp != TempDir+"/testdsmount" {
		t.Error("GetMountPoint not equal to the mountpoint set by CreateDataSet")
	}

	if err := ds.Destroy(); err != nil {
		t.Error(err)
	}

	empty, err := IsEmpty(TempDir + "/testdsmount")
	if err != nil {
		t.Error(err)
	}

	if !empty {
		t.Error("Newly created dataset is not empty")
	}
}

func TestCloneDataset(t *testing.T) {
	ds, err := CreateDataset("sjlTestPool/TestCloneSrc", nil)
	if err != nil {
		t.Error(err)
	}
	defer ds.Destroy()

	d1 := []byte("test\nfile\n")
	err = ioutil.WriteFile("/sjlTestPool/TestCloneSrc/testfile", d1, 0644)
	if err != nil {
		t.Error(err)
	}

	sn, err := ds.Snapshot("0")
	if err != nil {
		t.Error(err)
	}

	dsc, err := sn.Clone("sjlTestPool/TestCloneDst")
	if err != nil {
		t.Error(err)
	}
	defer dsc.Destroy()

	empty, err := IsEmpty("/sjlTestPool/TestCloneDst")
	if err != nil {
		t.Error(err)
	}

	if empty {
		t.Error("Cloned dataset is empty")
	}
}

func TestPromoteDataset(t *testing.T) {
	ds, err := CreateDataset("sjlTestPool/TestPromoteSrc", nil)
	if err != nil {
		t.Error(err)
	}
	// Don't defer, manually destroy it first, it should work since the destination is promoted
	//defer DestroyDataset("sjlTestPool/TestPromoteSrc")

	d1 := []byte("test\nfile\n")
	err = ioutil.WriteFile("/sjlTestPool/TestPromoteSrc/testfile", d1, 0644)
	if err != nil {
		t.Error(err)
	}

	sn, err := ds.Snapshot("0")
	if err != nil {
		t.Error(err)
	}

	dsc, err := sn.Clone("sjlTestPool/TestPromoteDst")
	if err != nil {
		t.Error(err)
	}
	defer dsc.Destroy()

	if err := dsc.Promote(); err != nil {
		t.Error(err)
	}

	empty, err := IsEmpty("/sjlTestPool/TestPromoteDst")
	if err != nil {
		t.Error(err)
	}

	if empty {
		t.Error("Promoted dataset is empty")
	}

	if err := ds.Destroy(); err != nil {
		t.Error("Can't destroy source after dst was promoted: ", err)
	}
}

func TestEmptyNonExist(t *testing.T) {
	if _, err := IsEmpty("/tmp/sjlEmptyNotExists"); err == nil {
		t.Error("IsEmpty on non-existent directory does not return err")
	}
}
