//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package collector

import (
	"os"
)

func lockJournal(string) (*os.File, error) {
	return nil, ErrJournalLockUnsupported
}

func syncJournalDirectory(string) error {
	return ErrJournalSyncUnsupported
}
