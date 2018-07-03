// Package cmd provides a thin wrapper around zfs commands
package cmd

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

func zExecNoOut(cmd []string) error {
	_, err := zExec(cmd)
	return err
}

type ZFSError struct {
	Cmd    string
	Stderr []byte
	err    string
}

func (err *ZFSError) Error() string {
	return err.Error()
}

func zExec(cmd []string) ([]byte, error) {
	ret, err := exec.Command("zfs", cmd...).Output()
	if err == nil {
		return ret, nil
	}
	rErr := &ZFSError{
		Cmd:    fmt.Sprintf("%#v", cmd),
		Stderr: err.(*exec.ExitError).Stderr,
		err:    err.Error(),
	}
	if eErr, ok := err.(*exec.ExitError); ok {
		rErr.Stderr = eErr.Stderr
	}
	return ret, rErr
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

func addCommaSeparated(cmd []string, properties []string) []string {
	cmd = addOpt(cmd, len(properties) > 0, strings.Join(properties, ","))
	return cmd
}

func addCommaSeparatedOption(cmd []string, option string, properties []string) []string {
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

// CreateFilesystemOpts are options that can be passed to CreateFilesystem
type CreateFilesystemOpts struct {
	SetProperties map[string]string
	DontMount     bool
	CreateParents bool
}

// CreateFilesystem runs zfs create
func CreateFilesystem(name string, opts *CreateFilesystemOpts) error {
	cmd := newCmd("create")
	if opts != nil {
		cmd = addOpt(cmd, opts.DontMount, "-u")
		cmd = addOpt(cmd, opts.CreateParents, "-p")
		cmd = addProperties(cmd, opts.SetProperties)
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// CreateVolumeOpts are options that can be passed to CreateVolume
type CreateVolumeOpts struct {
	SetProperties map[string]string
	CreateParents bool
	BlockSize     string
	Sparse        bool
}

// CreateVolue runs zfs create -V
func CreateVolue(name, size string, opts *CreateVolumeOpts) error {
	cmd := newCmd("create")
	cmd = addOpt(cmd, true, "-V", size)
	if opts != nil {
		cmd = addProperties(cmd, opts.SetProperties)
		cmd = addOpt(cmd, opts.BlockSize != "", "-b", opts.BlockSize)
		cmd = addOpt(cmd, opts.Sparse, "-s")
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// DestroyOpts are options that can be passed to Destroy
type DestroyOpts struct {
	DestroyChildren bool
	DestroyClones   bool
	ForceUnmount    bool
	Defer           bool
}

// Destroy runs zfs destroy
func Destroy(name string, opts *DestroyOpts) error {
	cmd := newCmd("destroy")
	if opts != nil {
		cmd = addOpt(cmd, opts.DestroyChildren, "-r")
		cmd = addOpt(cmd, opts.DestroyClones, "-R")
		cmd = addOpt(cmd, opts.ForceUnmount, "-f")
		cmd = addOpt(cmd, opts.Defer, "-d")
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// SnapshotOpts are options that can be passed to Snapshot
type SnapshotOpts struct {
	Recurse       bool
	SetProperties map[string]string
}

// Snapshot runs zfs snapshot
func Snapshot(name string, opts *SnapshotOpts) error {
	cmd := newCmd("snapshot")
	if opts != nil {
		cmd = addOpt(cmd, opts.Recurse, "-r")
		cmd = addProperties(cmd, opts.SetProperties)
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// RollbackOpts are options that can be passed to Rollback
type RollbackOpts struct {
	DestroyLaterSnapshots bool
	DestroyClones         bool
	ForceUnmount          bool
}

// Rollback runs zfs rollback
func Rollback(name string, opts *RollbackOpts) error {
	cmd := newCmd("rollback")
	if opts != nil {
		cmd = addOpt(cmd, opts.DestroyLaterSnapshots, "-r")
		cmd = addOpt(cmd, opts.DestroyClones, "-R")
		cmd = addOpt(cmd, opts.ForceUnmount, "-f")
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// CloneOpts are options that can be passed to Clone
type CloneOpts struct {
	CreateParents bool
	SetProperties map[string]string
}

// Clone runs zfs clone
func Clone(snapshot, name string, opts *CloneOpts) error {
	cmd := newCmd("clone")
	if opts != nil {
		cmd = addOpt(cmd, opts.CreateParents, "-p")
		cmd = addProperties(cmd, opts.SetProperties)
	}
	cmd = addNonEmpty(cmd, snapshot, name)
	return zExecNoOut(cmd)
}

// Promote runs zfs promote
func Promote(name string) error {
	cmd := newCmd("promote", name)
	return zExecNoOut(cmd)
}

// RenameOpts are options that can be passed to Rename
type RenameOpts struct {
	CreateParents bool
	DontReMount   bool
	ForceUnmount  bool
	Recurse       bool
}

// Rename runs zfs rename
func Rename(name, newName string, opts *RenameOpts) error {
	cmd := newCmd("rename")
	if opts != nil {
		cmd = addOpt(cmd, opts.CreateParents, "-p")
		cmd = addOpt(cmd, opts.DontReMount, "-u")
		cmd = addOpt(cmd, opts.ForceUnmount, "-f")
		cmd = addOpt(cmd, opts.Recurse, "-r")
	}
	cmd = addNonEmpty(cmd, name, newName)
	return zExecNoOut(cmd)
}

// ListOpts are options that can be passed to List
// Depth defaults to zero, set to -1 to omit the depth argument
type ListOpts struct {
	Recurse bool
	Depth   int
	Types   []string
}

// List runs zfs list
// Returns a map indexed by dataset name, which holds maps of the requested properties
func List(name string, opts *ListOpts) ([]string, error) {
	cmd := newCmd("list", "-Hp")
	if opts == nil {
		opts = &ListOpts{}
	}
	cmd = addOpt(cmd, opts.Recurse, "-r")
	cmd = addOpt(cmd, opts.Depth != -1, "-d", fmt.Sprintf("%d", opts.Depth))
	cmd = addCommaSeparatedOption(cmd, "-o", []string{"name"})
	cmd = addCommaSeparatedOption(cmd, "-t", opts.Types)
	out, err := zExec(cmd)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(out), "\n"), nil
}

// Set runs zfs set
func Set(name string, properties map[string]string) error {
	cmd := newCmd("set")
	cmd = addSetProperties(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// GetOpts are options that can be passed to Get
// Depth defaults to 0, set to -1 to omit the depth argument
type GetOpts struct {
	Recurse bool
	Depth   int
	Types   []string
	Sources []string
}

// Property is a zfs property recieved from Get
type Property struct {
	Value  string
	Source string
}

// Get runs zfs get
// Returns a map indexed by dataset, each of which holds a map indexed by the requested property
func Get(name string, properties []string, opts *GetOpts) (map[string]map[string]*Property, error) {
	cmd := newCmd("get", "-Hp")
	if opts == nil {
		opts = &GetOpts{}
	}

	if len(properties) == 0 {
		properties = []string{"name"}
	}
	cmd = addOpt(cmd, opts.Recurse, "-r")
	cmd = addOpt(cmd, opts.Depth != -1, "-d", fmt.Sprintf("%d", opts.Depth))
	cmd = addCommaSeparatedOption(cmd, "-t", opts.Types)
	cmd = addCommaSeparatedOption(cmd, "-s", opts.Sources)
	cmd = addCommaSeparated(cmd, properties)
	cmd = addNonEmpty(cmd, name)
	out, err := zExec(cmd)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]map[string]*Property)
	for _, l := range bytes.Split(out, []byte("\n")) {
		d := strings.Split(string(l), "\t")
		if len(d) < 4 {
			continue
		}
		if _, ok := ret[d[0]]; !ok {
			ret[d[0]] = make(map[string]*Property)
		}
		ret[d[0]][d[1]] = &Property{Value: d[2], Source: d[3]}
	}
	return ret, nil
}

// InheritOpts are options that can be passed to Inherit
type InheritOpts struct {
	Recurse  bool
	Received bool
}

// Inherit runs zfs inherit
func Inherit(name string, property string, opts *InheritOpts) error {
	cmd := newCmd("inherit")
	if opts != nil {
		cmd = addOpt(cmd, opts.Recurse, "-r")
		cmd = addOpt(cmd, opts.Received, "-S")
	}
	cmd = addNonEmpty(cmd, property, name)
	return zExecNoOut(cmd)
}

// MountEntry is a mounted zfs filesystem
type MountEntry struct {
	Name       string
	Mountpoint string
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
			Name:       strings.Trim(d[0], " "),
			Mountpoint: strings.Trim(d[1], " "),
		})
	}
	return ret, nil
}

// MountOpts are options that can be passed to Mount
type MountOpts struct {
	Properties []string
	MountAll   bool
}

// Mount runs zfs mount
func Mount(name string, opts *MountOpts) error {
	cmd := newCmd("mount")
	if opts != nil {
		cmd = addCommaSeparatedOption(cmd, "-o", opts.Properties)
		cmd = addOpt(cmd, opts.MountAll, "-a")
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// UnMountOpts are options that can be passed to UnMount
type UnMountOpts struct {
	UnMountAll   bool
	ForceUnmount bool
}

// UnMount runs zfs unmount
func UnMount(name string, opts *UnMountOpts) error {
	cmd := newCmd("unmount")
	if opts != nil {
		cmd = addOpt(cmd, opts.ForceUnmount, "-f")
		cmd = addOpt(cmd, opts.UnMountAll, "-a")
	}
	cmd = addNonEmpty(cmd, name)
	return zExecNoOut(cmd)
}

// Bookmark runs zfs bookmark
func Bookmark(snapshot, bookmark string) error {
	cmd := newCmd("bookmark", snapshot, bookmark)
	return zExecNoOut(cmd)
}
