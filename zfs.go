package zfs

import (
	"io"
	"os"
	"os/exec"
	"strings"
)

// DatasetExists checks for the existence of a dataset
func DatasetExists(dsName string) bool {
	err := exec.Command("zfs", "list", dsName).Run()
	if err != nil {
		return false
	}
	return true
}

// GetMountPoint returns the mountpoint for a dataset
func GetMountPoint(dsName string) (string, error) {
	out, err := exec.Command("zfs", "get", "-H",
		"-o", "value",
		"mountpoint", dsName).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// CreateDataset creates a dataset
func CreateDataset(dsName string, mountpoint string) error {
	if mountpoint != "" {
		return exec.Command("zfs", "create",
			"-o", "mountpoint="+mountpoint,
			dsName).Run()
	}
	return exec.Command("zfs", "create",
		dsName).Run()
}

// DestroyDataset destroys a dataset
func DestroyDataset(dsName string) error {
	return exec.Command("zfs", "destroy",
		"-R", dsName).Run()
}

// Snapshot takes a snapshot
func Snapshot(ds string) error {
	return exec.Command("zfs", "snapshot", ds).Run()
}

// CloneDataset clones a snapshot
func CloneDataset(src string, dst string) error {
	return exec.Command("zfs", "clone",
		src, dst).Run()
}

// PromoteDataset promotes a clone
func PromoteDataset(ds string) error {
	return exec.Command("zfs", "promote",
		ds).Run()
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
