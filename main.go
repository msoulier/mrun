package main

import (
	"os"
	"syscall"
	"path/filepath"
	"flag"
	"os/signal"

	"golang.org/x/sys/unix"
	"github.com/op/go-logging"
)

type Policy int64

const (
	Restart Policy = iota
	NoRestart
)

var (
	log	*logging.Logger = nil
	debug bool = false
	producer string = ""
	consumer string = ""
	policy Policy = Restart
	norestart bool = false
	shutdown_asap bool = false
	// number of retry attempts?
	// rate limiting?
)

func init() {
	flag.BoolVar(&debug, "debug", false, "Debug logging")
	flag.BoolVar(&norestart, "norestart", false, "Do not restart a failed process, just quit")
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

	if norestart {
		policy = NoRestart
	}
}

func watch_producer(pipefds [2]int, comms chan uintptr) {
	log.Debug("starting watch_producer")
	readfd := pipefds[0]
	writefd := pipefds[1]
	for {
		// Fork first process (writer - closes read end)
		pid1, _, errno := syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
		if errno != 0 {
			log.Errorf("Failed to fork first process: %v", errno)
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
		comms <- pid1
		go watch_consumer(pipefds, comms)

		var status syscall.WaitStatus
		syscall.Wait4(int(pid1), &status, 0, nil)
		log.Infof("Writer process (PID %d) exited with status %d", pid1, status.ExitStatus())

		comms <- 0
		return
	}
}

func watch_consumer(pipefds [2]int, comms chan uintptr) {
	log.Debug("starting watch_consumer")
	readfd := pipefds[0]
	writefd := pipefds[1]
	for {
		// Fork second process (reader - closes write end)
		pid2, _, errno := syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
		if errno != 0 {
			log.Errorf("Failed to fork second process: %v", errno)
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
		comms <- pid2

		var status syscall.WaitStatus
		syscall.Wait4(int(pid2), &status, 0, nil)
		log.Infof("Reader process (PID %d) exited with status %d", pid2, status.ExitStatus())

		comms <- 0
		return
	}
}

func main() {
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// Start signal handler
	go func() {
		sig := <-sigs
		switch sig {
		case syscall.SIGHUP:
			log.Warning("SIGHUP")
			shutdown_asap = true
		case syscall.SIGINT:
			log.Warning("SIGINT")
			shutdown_asap = true
		case syscall.SIGTERM:
			log.Warning("SIGTERM")
			shutdown_asap = true
		default:
			log.Debug("unknown signal")
		}
	}()

	for {
		if shutdown_asap {
			break
		}
		comms := make(chan uintptr)
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

		go watch_producer(pipefds, comms)

		log.Debug("main: top of for loop")
		// producer ready
		pid1 := <- comms
		log.Debugf("pid1: %d", pid1)
		// consumer ready
		pid2 := <- comms
		log.Debugf("pid2: %d", pid2)

		// Parent: close both ends, but not until both children
		// have forked.
		syscall.Close(readfd)
		syscall.Close(writefd)

		// Block on either goroutine quitting.
		<-comms
		log.Errorf("watch routine exited")
		syscall.Kill(int(pid1), syscall.SIGTERM)
		syscall.Kill(int(pid2), syscall.SIGTERM)

		if policy != Restart {
			os.Exit(1)
		}
	}
}
