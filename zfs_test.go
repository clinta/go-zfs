package zfs

import(
	"testing"
	"io/ioutil"
	"os"
	"os/exec"
)

var(
	TempDir string
)

func TestMain(m *testing.M){
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

	// Create a zfs pool for testing
	err = exec.Command( "zpool",
							"create",
							"sjlTestPool",
							poolFile.Name() ).Run()
	if err != nil {
		panic (err)
	}

	exitStatus := m.Run()

	exec.Command("zpool", "destroy", "-f", "sjlTestPool" ).Run()

	os.Remove(poolFile.Name())

	os.RemoveAll(TempDir)

	os.Exit(exitStatus)
}

func TestCreateDataset(t *testing.T) {
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

	if _, err := os.Stat(TempDir+"/testdsmount"); os.IsNotExist(err) {
		t.Error(TempDir+"/testdsmount", " does not exist after creating a dataset mounted there")
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

	empty, err := IsEmpty(TempDir+"/testdsmount")
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
