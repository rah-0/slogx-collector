//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"fmt"
	"os"
	"syscall"
)

func lockJournal(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, journalFileMode)
	if err != nil {
		return nil, journalError("open lock", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("journal: cannot acquire lock (another process may be using it): %w", err)
	}
	return file, nil
}

func syncJournalDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return journalError("open directory", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return journalError("sync directory", err)
	}
	return nil
}
