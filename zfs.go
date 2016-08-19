package zfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Dataset is a zfs dataset
type Dataset struct {
	Name string
}

// GetDataset returns a zfs dataset object from a string
func GetDataset(name string) (*Dataset, error) {
	if !DatasetExists(name) {
		return nil, fmt.Errorf("Dataset not found")
	}
	return &Dataset{
		Name: name,
	}, nil
}

func datasetList(name string) (ds []Dataset, err error) {
	out, err := exec.Command("zfs", "list", "-r", "-H", "-o", "name", "-t", "filesystem", name).Output()
	if err != nil {
		return
	}
	for _, d := range bytes.Split(out, []byte("\n")) {
		if strings.TrimSpace(string(d)) == name {
			continue
		}
		if strings.TrimSpace(string(d)) == "" {
			continue
		}
		ds = append(ds, Dataset{Name: strings.TrimSpace(string(d))})
	}
	return
}

// DatasetList lists all datasets
func DatasetList() ([]Dataset, error) {
	return datasetList("")
}

// DatasetExists checks for the existence of a dataset
func DatasetExists(name string) bool {
	err := exec.Command("zfs", "list", "-t", "filesystem", name).Run()
	return err == nil
}

// DatasetList lists all child datasets
func (ds *Dataset) DatasetList() ([]Dataset, error) {
	return datasetList(ds.Name)
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

// GetMountpoint returns the mountpoint for a dataset
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

// Destroy destroys a dataset
func (ds *Dataset) Destroy() error {
	if err := exec.Command("zfs", "destroy", "-R", ds.Name).Run(); err != nil {
		return err
	}
	ds = nil
	return nil
}

// GetSnapshot returns a snapshot from a string
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
	return err == nil
}

// Snapshot represents a zfs snapshot
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

// Promote promotes a cloned dataset
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
