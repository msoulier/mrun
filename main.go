package main

import (
	"os"
	"syscall"
	"path/filepath"
	"flag"

	"golang.org/x/sys/unix"
	"github.com/op/go-logging"
)

var (
	log	*logging.Logger = nil
	debug bool = false
	producer string = ""
	consumer string = ""
)

func init() {
	flag.BoolVar(&debug, "debug", false, "Debug logging")
	flag.StringVar(&producer, "producer", "", "Path to producer run script")
	flag.StringVar(&consumer, "consumer", "", "Path to consumer run script")
	flag.Parse()

	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000-0700} %{level} [%{shortfile}] %{message}`,
	)
	stderrBackend := logging.NewLogBackend(os.Stderr, "", 0)
	stderrFormatter := logging.NewBackendFormatter(stderrBackend, format)
	stderrBackendLevelled := logging.AddModuleLevel(stderrFormatter)
	logging.SetBackend(stderrBackendLevelled)
	if debug {
		stderrBackendLevelled.SetLevel(logging.DEBUG, "mrun")
	} else {
		stderrBackendLevelled.SetLevel(logging.INFO, "mrun")
	}
	log = logging.MustGetLogger("mrun")

	if producer == "" || consumer == "" {
		log.Error("The producer and consumer arguments are required")
		flag.PrintDefaults()
		os.Exit(1)
	} else {
		var err error
		producer, err = filepath.Abs(producer)
		if err != nil {
			panic(err)
		}
		log.Debugf("abs producer: %s", producer)
		consumer, err = filepath.Abs(consumer)
		if err != nil {
			panic(err)
		}
		log.Debugf("abs consumer: %s", consumer)
	}
}

func main() {
	// Create pipe
	pipefds := [2]int{}
	err := syscall.Pipe(pipefds[:])
	if err != nil {
		log.Errorf("Failed to create pipe: %v", err)
		os.Exit(1)
	}

	readfd := pipefds[0]
	writefd := pipefds[1]

	log.Debugf("Created pipe: read=%d, write=%d", readfd, writefd)

	// Fork first process (writer - closes read end)
	pid1, _, errno := syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
	if errno != 0 {
		log.Errorf("Failed to fork first process: %v", err)
		os.Exit(1)
	}

	if pid1 == 0 {
		log.Debug("in first child")
		// Child 1: writer process
		// Close read end
		syscall.Close(readfd)

		// Set write end to non-blocking
		flags, _ := unix.FcntlInt(uintptr(writefd), syscall.F_GETFL, 0)
		unix.FcntlInt(uintptr(writefd), syscall.F_SETFL, flags|syscall.O_NONBLOCK)

		// Redirect stdout to pipe write end
		syscall.Dup2(writefd, syscall.Stdout)
		syscall.Close(writefd)

		// Exec into program (generates data)
		//err := syscall.Exec("/bin/sh", []string{"sh", "-c", "echo 'Hello from writer'; seq 1 10"}, os.Environ())
		log.Debugf("calling exec on %s", producer)
		pname := filepath.Base(producer)
		err := syscall.Exec(producer, []string{pname}, os.Environ())
		if err != nil {
			log.Errorf("Exec producer failed: %v", err)
			os.Exit(1)
		}
	}

	// Fork second process (reader - closes write end)
	pid2, _, errno := syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
	if errno != 0 {
		log.Errorf("Failed to fork second process: %v", err)
		os.Exit(1)
	}

	if pid2 == 0 {
		log.Debug("in second child")
		// Child 2: reader process
		// Close write end
		syscall.Close(writefd)

		// Set read end to non-blocking
		flags, _ := unix.FcntlInt(uintptr(readfd), syscall.F_GETFL, 0)
		unix.FcntlInt(uintptr(readfd), syscall.F_SETFL, flags|syscall.O_NONBLOCK)

		// Redirect stdin from pipe read end
		syscall.Dup2(readfd, syscall.Stdin)
		syscall.Close(readfd)

		// Exec into program (reads data)
		log.Debugf("calling exec on %s", consumer)
		cname := filepath.Base(consumer)
		err := syscall.Exec(consumer, []string{cname}, os.Environ())
		if err != nil {
			log.Errorf("Exec consumer failed: %v", err)
			os.Exit(1)
		}
	}

	// Parent: close both ends
	syscall.Close(readfd)
	syscall.Close(writefd)

	// Wait for both children
	var status syscall.WaitStatus
	syscall.Wait4(int(pid1), &status, 0, nil)
	log.Infof("Writer process (PID %d) exited with status %d", pid1, status.ExitStatus())

	syscall.Wait4(int(pid2), &status, 0, nil)
	log.Infof("Reader process (PID %d) exited with status %d", pid2, status.ExitStatus())
}
