package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

// Frontend represents a connected ACP client.
type Frontend struct {
	id        int
	primary   bool
	scanner   *bufio.Scanner
	writer    io.Writer
	mu        sync.Mutex // protects writer and dead
	dead      bool       // set on first write error; skip further writes
	replayMu  sync.Mutex // protects replaying and pending
	replaying bool
	pending   [][]byte // live records queued behind the replay high-water mark
	done      chan struct{}
}

// Send writes a JSON line to this frontend. Thread-safe.
// Returns false if the frontend is dead (write error occurred).
func (f *Frontend) Send(line []byte) bool {
	f.replayMu.Lock()
	if f.replaying {
		f.pending = append(f.pending, append([]byte(nil), line...))
		f.replayMu.Unlock()
		return true
	}
	f.replayMu.Unlock()
	return f.sendSerialized(line)
}

func (f *Frontend) beginReplay() {
	f.replayMu.Lock()
	defer f.replayMu.Unlock()
	f.replaying = true
	f.pending = nil
}

func (f *Frontend) sendReplay(line []byte) bool {
	return f.sendSerialized(line)
}

func (f *Frontend) finishReplay() {
	f.replayMu.Lock()
	defer f.replayMu.Unlock()
	for _, line := range f.pending {
		if !f.sendSerialized(line) {
			break
		}
	}
	f.pending = nil
	f.replaying = false
}

func (f *Frontend) sendSerialized(line []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendLocked(line)
}

// sendLocked writes one record while f.mu is held.
func (f *Frontend) sendLocked(line []byte) bool {
	if f.dead {
		return false
	}

	// Combine line and newline into single write to prevent splitting
	msg := make([]byte, len(line)+1)
	copy(msg, line)
	msg[len(line)] = '\n'

	n, err := f.writer.Write(msg)
	if err != nil {
		log.Printf("frontend %d write error: %v", f.id, err)
		f.dead = true
		return false
	}
	if n != len(msg) {
		log.Printf("frontend %d short write: wrote %d/%d bytes", f.id, n, len(msg))
	}
	return true
}

// NewStdioFrontend creates a frontend connected to stdin/stdout.
func NewStdioFrontend(id int) *Frontend {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 200*1024*1024)
	return &Frontend{
		id:      id,
		primary: true,
		scanner: scanner,
		writer:  os.Stdout,
		done:    make(chan struct{}),
	}
}

// NewSocketFrontend creates a frontend from a Unix socket connection.
func NewSocketFrontend(id int, conn net.Conn) *Frontend {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 200*1024*1024)
	return &Frontend{
		id:      id,
		primary: false,
		scanner: scanner,
		writer:  conn,
		done:    make(chan struct{}),
	}
}

// ReadLines reads ndjson lines from the frontend and sends them to ch.
// Closes ch and done when the connection ends.
func (f *Frontend) ReadLines(ch chan<- FrontendMessage) {
	defer close(f.done)
	for f.scanner.Scan() {
		line := make([]byte, len(f.scanner.Bytes()))
		copy(line, f.scanner.Bytes())
		ch <- FrontendMessage{Frontend: f, Line: line}
	}
	if err := f.scanner.Err(); err != nil {
		log.Printf("frontend %d read error: %v", f.id, err)
	}
}

// FrontendMessage pairs a raw JSON line with the frontend that sent it.
type FrontendMessage struct {
	Frontend *Frontend
	Line     []byte
}
