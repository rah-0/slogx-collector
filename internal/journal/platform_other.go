//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package journal

import (
	"os"
)

func lockJournal(string) (*os.File, error) {
	return nil, ErrLockUnsupported
}

func syncJournalDirectory(string) error {
	return ErrSyncUnsupported
}
