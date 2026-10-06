package broker

import (
	"errors"
	"fmt"
	"strings"
)

// The wire protocol is newline-delimited text. TCP is a byte stream, not a
// sequence of messages, so we need some way to tell where one command ends
// and the next begins. A newline is the simplest framing that still lets you
// talk to the broker by hand with `nc`.
//
//	client → broker:  SUB <topic>
//	                  UNSUB <topic>
//	                  PUB <topic> <payload>
//	broker → client:  MSG <topic> <payload>
//	                  OK
//	                  ERR <reason>

const (
	OpSub   = "SUB"
	OpUnsub = "UNSUB"
	OpPub   = "PUB"
)

var ErrEmptyCommand = errors.New("empty command")

// Command is one parsed line from a client.
type Command struct {
	Op      string
	Topic   string
	Payload string // only set for PUB
}

// ParseCommand turns a raw line (without the trailing newline) into a Command.
func ParseCommand(line string) (Command, error) {
	line = strings.TrimRight(line, "\r") // tolerate clients that send \r\n
	if strings.TrimSpace(line) == "" {
		return Command{}, ErrEmptyCommand
	}

	// Split into at most 3 parts so the payload can contain spaces.
	parts := strings.SplitN(line, " ", 3)
	op := strings.ToUpper(parts[0])

	switch op {
	case OpSub, OpUnsub:
		if len(parts) != 2 || parts[1] == "" {
			return Command{}, fmt.Errorf("usage: %s <topic>", op)
		}
		return Command{Op: op, Topic: parts[1]}, nil

	case OpPub:
		if len(parts) != 3 || parts[1] == "" {
			return Command{}, fmt.Errorf("usage: %s <topic> <payload>", op)
		}
		return Command{Op: op, Topic: parts[1], Payload: parts[2]}, nil

	default:
		return Command{}, fmt.Errorf("unknown command %q", parts[0])
	}
}

// FormatMessage builds the line the broker sends to subscribers.
func FormatMessage(topic, payload string) string {
	return "MSG " + topic + " " + payload + "\n"
}
