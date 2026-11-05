//go:build !windows

package mediaguard

import "syscall"

// freeBytes reports the bytes available to unprivileged users on the filesystem holding dir
// (statfs f_bavail * f_bsize — the same number `df` prints under "Avail").
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
