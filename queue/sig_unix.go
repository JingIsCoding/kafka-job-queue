// +build linux freebsd netbsd openbsd dragonfly solaris illumos aix darwin

package queue

import (
	"os"
	"os/signal"
	"syscall"
)

var (
	SIGTERM os.Signal = syscall.SIGTERM
	SIGINT  os.Signal = os.Interrupt

	signalMap = map[os.Signal]string{
		SIGTERM: "terminate",
		SIGINT:  "terminate",
	}
)

func hookSignals() chan os.Signal {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, SIGINT)
	signal.Notify(sigchan, SIGTERM)
	return sigchan
}
