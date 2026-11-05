//go:build windows

package mediaguard

import "errors"

// freeBytes is not implemented on Windows (the engine only ships on Linux). Fail closed: the disk
// guard reports "low" so nothing is written without a real measurement.
func freeBytes(dir string) (uint64, error) {
	return 0, errors.New("mediaguard: disk probe not supported on windows")
}
