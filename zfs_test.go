package zfs

import (
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

var (
	TempDir string
)

func TestMain(m *testing.M) {
	var err error
	TempDir, err = ioutil.TempDir("", "sjlTest")
	if err != nil {
		panic(err)
	}

	poolFile, err := ioutil.TempFile(TempDir, "pool")
	if err != nil {
		panic(err)
	}

	// Make file 2GB
	if err := poolFile.Truncate(2e9); err != nil {
		panic(err)
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
			panic(err)
		}
		e = poolCreate.Run()
		if e != nil {
			panic(err)
		}
	}

	exitStatus := m.Run()

	poolDestroy.Run()

	os.Remove(poolFile.Name())

	os.RemoveAll(TempDir)

	os.Exit(exitStatus)
}

func TestCreateDataset(t *testing.T) {
	if DatasetExists("sjlTestPool/TestCreateDataset") {
		t.Error("sjlTestPool/TestCreateDataset exists before creating it")
	}

	if err := CreateDataset("sjlTestPool/TestCreateDataset", ""); err != nil {
		t.Error(err)
	}

	if !DatasetExists("sjlTestPool/TestCreateDataset") {
		t.Error("sjlTestPool/TestCreateDataset dataset does not exist after creating it")
	}

	if err := DestroyDataset("sjlTestPool/TestCreateDataset"); err != nil {
		t.Error(err)
	}

}

func TestCreateDatasetMount(t *testing.T) {
	if err := CreateDataset("sjlTestPool/TestCreateDatasetMount", TempDir+"/testdsmount"); err != nil {
		t.Error(err)
	}

	if !DatasetExists("sjlTestPool/TestCreateDatasetMount") {
		t.Error("sjlTestPool/TestCreateDatasetMount dataset does not exist after creating it")
	}

	if _, err := os.Stat(TempDir + "/testdsmount"); os.IsNotExist(err) {
		t.Error(TempDir+"/testdsmount", " does not exist after creating a dataset mounted there")
	}

	_, err := GetMountPoint("sjlTestPool/TestCreateDatasetMount2")
	if err == nil {
		t.Error("GetMountPoint for non-existent datastore did not return err")
	}

	mp, err := GetMountPoint("sjlTestPool/TestCreateDatasetMount")
	if err != nil {
		t.Error(err)
	}
	if mp != TempDir+"/testdsmount" {
		t.Error("GetMountPoint not equal to the mountpoint set by CreateDataSet")
	}

	if err := DestroyDataset("sjlTestPool/TestCreateDatasetMount"); err != nil {
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
	if err := CreateDataset("sjlTestPool/TestCloneSrc", ""); err != nil {
		t.Error(err)
	}
	defer DestroyDataset("sjlTestPool/TestCloneSrc")

	d1 := []byte("test\nfile\n")
	err := ioutil.WriteFile("/sjlTestPool/TestCloneSrc/testfile", d1, 0644)
	if err != nil {
		t.Error(err)
	}

	if err := Snapshot("sjlTestPool/TestCloneSrc@0"); err != nil {
		t.Error(err)
	}

	if err := CloneDataset("sjlTestPool/TestCloneSrc@0", "sjlTestPool/TestCloneDst"); err != nil {
		t.Error(err)
	}
	defer DestroyDataset("sjlTestPool/TestCloneDst")

	empty, err := IsEmpty("/sjlTestPool/TestCloneDst")
	if err != nil {
		t.Error(err)
	}

	if empty {
		t.Error("Cloned dataset is empty")
	}
}

func TestPromoteDataset(t *testing.T) {
	if err := CreateDataset("sjlTestPool/TestPromoteSrc", ""); err != nil {
		t.Error(err)
	}
	// Don't defer, manually destroy it first, it should work since the destination is promoted
	//defer DestroyDataset("sjlTestPool/TestPromoteSrc")

	d1 := []byte("test\nfile\n")
	err := ioutil.WriteFile("/sjlTestPool/TestPromoteSrc/testfile", d1, 0644)
	if err != nil {
		t.Error(err)
	}

	if err := Snapshot("sjlTestPool/TestPromoteSrc@0"); err != nil {
		t.Error(err)
	}

	if err := CloneDataset("sjlTestPool/TestPromoteSrc@0", "sjlTestPool/TestPromoteDst"); err != nil {
		t.Error(err)
	}
	defer DestroyDataset("sjlTestPool/TestPromoteDst")

	if err := PromoteDataset("sjlTestPool/TestPromoteDst"); err != nil {
		t.Error(err)
	}

	empty, err := IsEmpty("/sjlTestPool/TestPromoteDst")
	if err != nil {
		t.Error(err)
	}

	if empty {
		t.Error("Promoted dataset is empty")
	}

	if err := DestroyDataset("sjlTestPool/TestPromoteSrc"); err != nil {
		t.Error("Can't destroy source after dst was promoted: ", err)
	}
}

func TestEmptyNonExist(t *testing.T) {
	if _, err := IsEmpty("/tmp/sjlEmptyNotExists"); err == nil {
		t.Error("IsEmpty on non-existent directory does not return err")
	}
}
