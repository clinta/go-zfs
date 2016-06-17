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

func DatasetExists(dsName string) bool {
	err := exec.Command(zfsExec, "list", dsName).Run()
	if err != nil {
		return false
	}
	return true
}

func GetMountPoint(dsName string) (string, error) {
	out, err := exec.Command(zfsExec, "get", "-H",
		"-o", "value",
		"mountpoint", dsName).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func CreateDataset(dsName string, mountpoint string) error {
	if mountpoint != "" {
		return exec.Command(zfsExec, "create",
			"-o", "mountpoint="+mountpoint,
			dsName).Run()
	}
	return exec.Command(zfsExec, "create",
		dsName).Run()
}

func DestroyDataset(dsName string) error {
	return exec.Command(zfsExec, "destroy",
		"-R", dsName).Run()
}

func Snapshot(ds string) error {
	return exec.Command(zfsExec, "snapshot", ds).Run()
}

func CloneDataset(src string, dst string) error {
	return exec.Command(zfsExec, "clone",
		src, dst).Run()
}

func PromoteDataset(ds string) error {
	return exec.Command(zfsExec, "promote",
		ds).Run()
}

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
