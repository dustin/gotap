package tap

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type TapOperation struct {
	OpCode            gomemcached.CommandCode
	Status            gomemcached.Status
	Cas               uint64
	Extras, Key, Body []byte
}

func (op *TapOperation) ToString() (rv string) {
	typeMap := map[gomemcached.CommandCode]string{
		gomemcached.TAP_CONNECT:          "CONNECT",
		gomemcached.TAP_MUTATION:         "MUTATION",
		gomemcached.TAP_DELETE:           "DELETE",
		gomemcached.TAP_FLUSH:            "FLUSH",
		gomemcached.TAP_OPAQUE:           "OPAQUE",
		gomemcached.TAP_VBUCKET_SET:      "VBUCKET_SET",
		gomemcached.TAP_CHECKPOINT_START: "CHECKPOINT_START",
		gomemcached.TAP_CHECKPOINT_END:   "CHECKPOINT_END"}

	types := typeMap[op.OpCode]
	if types == "" {
		types = fmt.Sprintf("<unknown 0x%x>", op.OpCode)
	}

	rv = fmt.Sprintf("<TapOperation %s, key='%s' (%d bytes)>",
		types, op.Key, len(op.Body))

	return rv
}

type TapArguments struct {
	Backfill         uint64
	Dump             bool
	VBuckets         []uint16
	Takeover         bool
	SupportAck       bool
	KeysOnly         bool
	Checkpoint       bool
	ClientName       string
	RegisteredClient bool
}

func (args *TapArguments) Flags() (rv uint32) {
	rv = 0
	if args.Backfill != 0 {
		rv |= gomemcached.BACKFILL
	}
	if args.Dump {
		rv |= gomemcached.DUMP
	}
	if len(args.VBuckets) > 0 {
		rv |= gomemcached.LIST_VBUCKETS
	}
	if args.Takeover {
		rv |= gomemcached.TAKEOVER_VBUCKETS
	}
	if args.SupportAck {
		rv |= gomemcached.SUPPORT_ACK
	}
	if args.KeysOnly {
		rv |= gomemcached.REQUEST_KEYS_ONLY
	}
	if args.Checkpoint {
		rv |= gomemcached.CHECKPOINT
	}
	if args.RegisteredClient {
		rv |= gomemcached.REGISTERED_CLIENT
	}
	return rv
}

func (args *TapArguments) Bytes() (rv []byte) {
	buf := bytes.NewBuffer([]byte{})

	if args.Backfill > 0 {
		binary.Write(buf, binary.BigEndian, uint64(args.Backfill))
	}

	if len(args.VBuckets) > 0 {
		binary.Write(buf, binary.BigEndian, uint16(len(args.VBuckets)))
		for i := 0; i < len(args.VBuckets); i++ {
			binary.Write(buf, binary.BigEndian, uint16(args.VBuckets[i]))
		}
	}
	return buf.Bytes()
}

func handleFeed(mc *memcached.Client, ch chan TapOperation) {
	defer close(ch)
	for {
		pkt, err := mc.Receive()
		if err != nil {
			return
		}
		to := TapOperation{
			OpCode: pkt.Opcode,
			Status: pkt.Status,
			Cas:    pkt.Cas,
			Extras: pkt.Extras,
			Key:    pkt.Key,
			Body:   pkt.Body,
		}
		ch <- to
	}
}

func Feed(mc *memcached.Client, args TapArguments) (chan TapOperation, error) {
	err := mc.Transmit(&gomemcached.MCRequest{
		Opcode: gomemcached.TAP_CONNECT,
		Key:    []byte(args.ClientName),
		Extras: make([]byte, uint32(args.Flags())),
		Body:   args.Bytes()})
	if err != nil {
		return nil, err
	}

	ch := make(chan TapOperation)
	go handleFeed(mc, ch)
	return ch, nil
}
