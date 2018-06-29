package zfs

import (
	"fmt"
	"os/exec"
	"strings"
)

func zExecNoOut(cmd []string) error {
	_, err := zExec(cmd)
	return err
}

func zExec(cmd []string) ([]byte, error) {
	return exec.Command("zfs", cmd...).Output()
}

func newCmd(args ...string) []string {
	return args
}

func addProperties(cmd []string, properties map[string]string) []string {
	if properties == nil {
		return cmd
	}
	for k, v := range properties {
		cmd = append(cmd, "-o", fmt.Sprintf("%s=%s", k, v))
	}
	return cmd
}

func addSetProperties(cmd []string, properties map[string]string) []string {
	if properties == nil {
		return cmd
	}
	for k, v := range properties {
		cmd = append(cmd, fmt.Sprintf("%s=%s", k, v))
	}
	return cmd
}

func addOpt(cmd []string, b bool, o ...string) []string {
	if b {
		cmd = append(cmd, o...)
	}
	return cmd
}

func addNonEmpty(cmd []string, s ...string) []string {
	for _, ss := range s {
		cmd = addOpt(cmd, ss != "", ss)
	}
	return cmd
}

func createFilesystem(name string, properties map[string]string, noMount, createParents bool) error {
	cmd := newCmd("create")
	cmd = addOpt(cmd, noMount, "-u")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func createVolue(name, size string, properties map[string]string, blockSize string, sparse, createParents bool) error {
	cmd := newCmd("create")
	cmd = addOpt(cmd, true, "-V", size)
	cmd = addProperties(cmd, properties)
	cmd = addOpt(cmd, blockSize != "", "-b", blockSize)
	cmd = addOpt(cmd, sparse, "-s")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func destroy(name string, destroyChildren, destroyClones, forceUnmount, deferDestroy bool) error {
	cmd := newCmd("destroy")
	cmd = addOpt(cmd, destroyChildren, "-r")
	cmd = addOpt(cmd, destroyClones, "-R")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addOpt(cmd, deferDestroy, "-d")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func snapshot(name string, recurse bool, properties map[string]string) error {
	cmd := newCmd("snapshot")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func rollback(name string, destroyLater, destroyClones, forceUnmount bool) error {
	cmd := newCmd("rollback")
	cmd = addOpt(cmd, destroyLater, "-r")
	cmd = addOpt(cmd, destroyClones, "-R")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func clone(snapshot, name string, createParents bool, properties map[string]string) error {
	cmd := newCmd("clone")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, snapshot, name)
	return zExecNoOut(cmd)
}

func promote(name string) error {
	cmd := newCmd("promote", name)
	return zExecNoOut(cmd)
}

func rename(name, newName string, createParents, noReMount, forceUnmount, recurse bool) error {
	cmd := newCmd("rename")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addOpt(cmd, noReMount, "-u")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addNonEmpty(cmd, name, newName)
	return zExecNoOut(cmd)
}

// Depth -1 to omit depth
func list(name string, recurse bool, depth int, properties []string, types []string, sortBy string, sortDescending bool) error {
	//TODO
	return fmt.Errorf("Not Implimented")
}

func set(name string, properties map[string]string) error {
	cmd := newCmd("set")
	cmd = addSetProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func get(name string, property []string, recurse bool, depth int, types []string, source []string) error {
	//TODO
	return fmt.Errorf("Not Implimented")
}

func inherit(name string, property string, recurse, received bool) error {
	cmd := newCmd("inherit")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addOpt(cmd, received, "-S")
	cmd = addNonEmpty(cmd, property, name)
	return zExecNoOut(cmd)
}

func getMounts() error {
	//TODO
	return fmt.Errorf("Not Implimented")
}

func mount(name string, properties []string, all bool) error {
	cmd := newCmd("mount")
	if len(properties) > 0 {
		cmd = addOpt(cmd, true, "-o", strings.Join(properties, ","))
	}
	cmd = addOpt(cmd, all, "-a")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func unMount(name string, all, force bool) error {
	cmd := newCmd("unmount")
	cmd = addOpt(cmd, force, "-f")
	cmd = addOpt(cmd, all, "-a")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

func bookmark(snapshot, bookmark string) error {
	cmd := newCmd("bookmark", snapshot, bookmark)
	return zExecNoOut(cmd)
}
