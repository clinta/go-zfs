package zfs

import(
	"os/exec"
	"os"
	"io"
	"strings"
	log "github.com/Sirupsen/logrus"
)

var(
	zfsExec	string
)

type dataset struct{
}

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
	out, err := exec.Command(	zfsExec, "get", "-H",
								"-o", "value",
								"mountpoint", dsName	).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func CreateDatasetMount(dsName string, mountpoint string) error {
	return exec.Command(	zfsExec, "create", 
							"-o", "mountpoint="+mountpoint,
							dsName							).Run()
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
