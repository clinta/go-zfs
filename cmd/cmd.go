// Package cmd provides a thin wrapper around zfs commands
package cmd

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

func addCommaSeparated(cmd []string, option string, properties []string) []string {
	cmd = addOpt(cmd, len(properties) > 0, option, strings.Join(properties, ","))
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

// CreateFilesystem runs zfs create
func CreateFilesystem(name string, properties map[string]string, noMount, createParents bool) error {
	cmd := newCmd("create")
	cmd = addOpt(cmd, noMount, "-u")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// CreateVolue runs zfs create -V
func CreateVolue(name, size string, properties map[string]string, blockSize string, sparse, createParents bool) error {
	cmd := newCmd("create")
	cmd = addOpt(cmd, true, "-V", size)
	cmd = addProperties(cmd, properties)
	cmd = addOpt(cmd, blockSize != "", "-b", blockSize)
	cmd = addOpt(cmd, sparse, "-s")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Destroy runs zfs destroy
func Destroy(name string, destroyChildren, destroyClones, forceUnmount, deferDestroy bool) error {
	cmd := newCmd("destroy")
	cmd = addOpt(cmd, destroyChildren, "-r")
	cmd = addOpt(cmd, destroyClones, "-R")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addOpt(cmd, deferDestroy, "-d")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Snapshot runs zfs snapshot
func Snapshot(name string, recurse bool, properties map[string]string) error {
	cmd := newCmd("snapshot")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Rollback runs zfs rollback
func Rollback(name string, destroyLater, destroyClones, forceUnmount bool) error {
	cmd := newCmd("rollback")
	cmd = addOpt(cmd, destroyLater, "-r")
	cmd = addOpt(cmd, destroyClones, "-R")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Clone runs zfs clone
func Clone(snapshot, name string, createParents bool, properties map[string]string) error {
	cmd := newCmd("clone")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addProperties(cmd, properties)
	cmd = addNonEmpty(cmd, snapshot, name)
	return zExecNoOut(cmd)
}

// Promote runs zfs promote
func Promote(name string) error {
	cmd := newCmd("promote", name)
	return zExecNoOut(cmd)
}

// Rename runs zfs rename
func Rename(name, newName string, createParents, noReMount, forceUnmount, recurse bool) error {
	cmd := newCmd("rename")
	cmd = addOpt(cmd, createParents, "-p")
	cmd = addOpt(cmd, noReMount, "-u")
	cmd = addOpt(cmd, forceUnmount, "-f")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addNonEmpty(cmd, name, newName)
	return zExecNoOut(cmd)
}

// List runs zfs list
// call with depth of -1 to omit the depth argument
// Returns [][]string, where the first dimension is the row, second is the column
func List(name string, recurse bool, depth int, properties []string, types []string, sortBy string, sortDescending bool) ([][]string, error) {
	cmd := newCmd("list", "-Hp")
	if len(properties) == 0 {
		properties = []string{"name"}
	}
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addOpt(cmd, depth != -1, "-d", fmt.Sprintf("%s", depth))
	cmd = addCommaSeparated(cmd, "-o", properties)
	cmd = addOpt(cmd, sortBy != "" && !sortDescending, "-s", sortBy)
	cmd = addOpt(cmd, sortBy != "" && sortDescending, "-S", sortBy)
	cmd = addCommaSeparated(cmd, "-t", types)
	out, err := zExec(cmd)
	if err != nil {
		return nil, err
	}
	ret := [][]string{}
	for _, l := range strings.Split(string(out), "\n") {
		ret = append(ret, strings.Split(l, "\t"))
	}
	return ret, nil
}

// Set runs zfs set
func Set(name string, properties map[string]string) error {
	cmd := newCmd("set")
	cmd = addSetProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

type Property struct {
	name     string
	property string
	value    string
	source   string
}

// Get runs zfs get
// call with depth of -1 to omit the depth argument
func Get(name string, property []string, recurse bool, depth int, types []string, source []string) ([]*Property, error) {
	cmd := newCmd("get", "-Hp")
	if len(property) == 0 {
		property = []string{"name"}
	}
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addOpt(cmd, depth != -1, "-d", fmt.Sprintf("%s", depth))
	cmd = addCommaSeparated(cmd, "-t", types)
	cmd = addCommaSeparated(cmd, "-s", source)
	cmd = addCommaSeparated(cmd, "", property)
	out, err := zExec(cmd)
	if err != nil {
		return nil, err
	}
	ret := []*Property{}
	for _, l := range strings.Split(string(out), "\n") {
		d := strings.Split(l, "\t")
		ret = append(ret, &Property{
			name:     d[0],
			property: d[1],
			value:    d[2],
			source:   d[3],
		})
	}
	return ret, nil
}

// Inherit runs zfs inherit
func Inherit(name string, property string, recurse, received bool) error {
	cmd := newCmd("inherit")
	cmd = addOpt(cmd, recurse, "-r")
	cmd = addOpt(cmd, received, "-S")
	cmd = addNonEmpty(cmd, property, name)
	return zExecNoOut(cmd)
}

// Mount represents a mounted zfs filesystem
type MountEntry struct {
	name       string
	mountpoint string
}

// GetMounts runs zfs mount with no arguments
func GetMounts() ([]*MountEntry, error) {
	cmd := newCmd("mount")
	out, err := zExec(cmd)
	if err != nil {
		return nil, err
	}
	ret := []*MountEntry{}
	for _, l := range strings.Split(string(out), "\n") {
		d := strings.SplitN(l, "  ", 2)
		ret = append(ret, &MountEntry{
			name:       strings.Trim(d[0], " "),
			mountpoint: strings.Trim(d[1], " "),
		})
	}
	return ret, nil
}

// Mount runs zfs mount
func Mount(name string, properties []string, all bool) error {
	cmd := newCmd("mount")
	cmd = addCommaSeparated(cmd, "-o", properties)
	cmd = addOpt(cmd, all, "-a")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// UnMount runs zfs unmount
func UnMount(name string, all, force bool) error {
	cmd := newCmd("unmount")
	cmd = addOpt(cmd, force, "-f")
	cmd = addOpt(cmd, all, "-a")
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Bookmark runs zfs bookmark
func Bookmark(snapshot, bookmark string) error {
	cmd := newCmd("bookmark", snapshot, bookmark)
	return zExecNoOut(cmd)
}
