package zfs

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type Dataset struct {
	Name string
}

func GetDataset(name string) (*Dataset, error) {
	if !DatasetExists(name) {
		return nil, fmt.Errorf("Dataset not found")
	}
	return &Dataset{
		Name: name,
	}, nil
}

// DatasetExists checks for the existence of a dataset
func DatasetExists(name string) bool {
	err := exec.Command("zfs", "list", "-t", "filesystem", name).Run()
	if err != nil {
		return false
	}
	return true
}

// GetProperty returns a property for a dataset
func (ds *Dataset) GetProperty(property string) (string, error) {
	out, err := exec.Command("zfs", "get", "-H",
		"-o", "value",
		property, ds.Name).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// MountPoint returns the mountpoint for a dataset
func (ds *Dataset) GetMountpoint() (string, error) {
	return ds.GetProperty("mountpoint")
}

// CreateDataset creates a dataset
func CreateDataset(name string, properties map[string]string) (*Dataset, error) {
	args := []string{"create"}
	if properties != nil {
		for n, v := range properties {
			args = append(args, "-o", fmt.Sprintf("%v=%v", n, v))
		}
	}
	args = append(args, name)
	err := exec.Command("zfs", args...).Run()
	if err != nil {
		return nil, err
	}
	return GetDataset(name)
}

// DestroyDataset destroys a dataset
func (ds *Dataset) Destroy() error {
	if err := exec.Command("zfs", "destroy", "-R", ds.Name).Run(); err != nil {
		return err
	}
	ds = nil
	return nil
}

func GetSnapshot(name string) (*Snapshot, error) {
	if !SnapshotExists(name) {
		return nil, fmt.Errorf("Snapshot not found")
	}
	return &Snapshot{
		Name: name,
	}, nil
}

// SnapshotExists checks for the existence of a dataset
func SnapshotExists(Name string) bool {
	err := exec.Command("zfs", "list", "-t", "snapshot", Name).Run()
	if err != nil {
		return false
	}
	return true
}

type Snapshot struct {
	Name string
}

// Snapshot creates a snapshot
func (ds *Dataset) Snapshot(name string) (*Snapshot, error) {
	sn := fmt.Sprintf("%v@%v", ds.Name, name)
	err := exec.Command("zfs", "snapshot", sn).Run()
	if err != nil {
		return nil, err
	}
	return GetSnapshot(sn)
}

// Clone clones a snapshot
func (sn *Snapshot) Clone(target string) (*Dataset, error) {
	err := exec.Command("zfs", "clone", sn.Name, target).Run()
	if err != nil {
		return nil, err
	}
	return GetDataset(target)
}

// PromoteDataset promotes a clone
func (ds *Dataset) Promote() error {
	return exec.Command("zfs", "promote", ds.Name).Run()
}

// IsEmpty returns whether or not a directory is empty
func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
