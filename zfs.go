package zfs

import (
	log "github.com/Sirupsen/logrus"
	"io"
	"os"
	"os/exec"
	"strings"
)

var (
	zfsExec string
)

func init() {
	var err error
	zfsExec, err = exec.LookPath("zfs")
	if err != nil {
		log.Error(err)
		log.Fatal("zfs Executable not found")
	}
}

// DatasetExists checks for the existence of a dataset
func DatasetExists(dsName string) bool {
	err := exec.Command(zfsExec, "list", dsName).Run()
	if err != nil {
		return false
	}
	return true
}

// GetMountPoint returns the mountpoint for a dataset
func GetMountPoint(dsName string) (string, error) {
	out, err := exec.Command(zfsExec, "get", "-H",
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
		return exec.Command(zfsExec, "create",
			"-o", "mountpoint="+mountpoint,
			dsName).Run()
	}
	return exec.Command(zfsExec, "create",
		dsName).Run()
}

// DestroyDataset destroys a dataset
func DestroyDataset(dsName string) error {
	return exec.Command(zfsExec, "destroy",
		"-R", dsName).Run()
}

// Snapshot takes a snapshot
func Snapshot(ds string) error {
	return exec.Command(zfsExec, "snapshot", ds).Run()
}

// CloneDataset clones a snapshot
func CloneDataset(src string, dst string) error {
	return exec.Command(zfsExec, "clone",
		src, dst).Run()
}

// PromoteDataset promotes a clone
func PromoteDataset(ds string) error {
	return exec.Command(zfsExec, "promote",
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
