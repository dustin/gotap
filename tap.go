package tap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

var bigEndian = binary.BigEndian

type TapOperation struct {
	OpCode            gomemcached.CommandCode
	Status            uint16
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

type TapClient struct {
	Conn   net.Conn
	writer *bufio.Writer
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

func (args *TapArguments) Body() (rv []byte) {
	buf := bytes.NewBuffer([]byte{})

	if args.Backfill > 0 {
		binary.Write(buf, bigEndian, uint64(args.Backfill))
	}

	if len(args.VBuckets) > 0 {
		binary.Write(buf, bigEndian, uint16(len(args.VBuckets)))
		for i := 0; i < len(args.VBuckets); i++ {
			binary.Write(buf, bigEndian, uint16(args.VBuckets[i]))
		}
	}
	return buf.Bytes()
}

func (client *TapClient) handleFeed(ch chan TapOperation) {
	defer close(ch)
	for {
		ch <- getResponse(client)
	}
}

func (client *TapClient) AuthPlain(u, p string) error {
	mc, err := memcached.Wrap(client.Conn)
	if err != nil {
		return err
	}
	_, err = mc.Auth(u, p)
	if err != nil {
		return err
	}
	return nil
}

func (client *TapClient) Feed() (ch chan TapOperation) {
	ch = make(chan TapOperation)
	go client.handleFeed(ch)
	return ch
}

func transmitRequest(o *bufio.Writer, req gomemcached.MCRequest) {
	// 0
	binary.Write(o, bigEndian, uint8(gomemcached.REQ_MAGIC))
	binary.Write(o, bigEndian, uint8(req.Opcode))
	binary.Write(o, bigEndian, uint16(len(req.Key)))
	// 4
	binary.Write(o, bigEndian, uint8(len(req.Extras)))
	binary.Write(o, bigEndian, uint8(0))
	binary.Write(o, bigEndian, uint16(req.VBucket))
	// 8
	binary.Write(o, bigEndian, uint32(len(req.Body)+
		len(req.Key)+
		len(req.Extras)))
	// 12
	binary.Write(o, bigEndian, uint32(req.Opaque))
	// 16
	binary.Write(o, bigEndian, uint64(req.Cas))
	// The rest
	binary.Write(o, bigEndian, req.Extras)
	binary.Write(o, bigEndian, req.Key)
	binary.Write(o, bigEndian, req.Body)
	o.Flush()
}

func (client *TapClient) Start(args TapArguments) {
	var req gomemcached.MCRequest
	req.Opcode = gomemcached.TAP_CONNECT
	req.Key = []byte(args.ClientName)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = make([]byte, uint32(args.Flags()))
	bigEndian.PutUint32(req.Extras, uint32(args.Flags()))
	req.Body = args.Body()
	transmitRequest(client.writer, req)
}

func Connect(prot string, dest string) (rv *TapClient) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}
	rv = new(TapClient)
	rv.Conn = conn
	rv.writer = bufio.NewWriterSize(rv.Conn, 256)

	return rv
}

func writeBytes(s *bufio.Writer, data []byte) {
	if len(data) > 0 {
		written, err := s.Write(data)
		if err != nil || written != len(data) {
			log.Printf("Error writing bytes:  %s", err)
			runtime.Goexit()
		}
	}
	return

}

func readOb(s net.Conn, buf []byte) {
	x, err := io.ReadFull(s, buf)
	if err != nil || x != len(buf) {
		log.Printf("Error reading part: %s", err)
		runtime.Goexit()
	}
}

func getResponse(client *TapClient) TapOperation {
	hdrBytes := make([]byte, gomemcached.HDR_LEN)
	bytesRead, err := io.ReadFull(client.Conn, hdrBytes)
	if err != nil || bytesRead != gomemcached.HDR_LEN {
		log.Printf("Error reading message: %s (%d bytes)", err, bytesRead)
		runtime.Goexit()
	}
	res := grokHeader(hdrBytes)
	readContents(client.Conn, res)
	return res
}

func readContents(s net.Conn, res TapOperation) {
	readOb(s, res.Extras)
	readOb(s, res.Key)
	readOb(s, res.Body)
}

func grokHeader(hdrBytes []byte) (rv TapOperation) {
	if hdrBytes[0] != gomemcached.REQ_MAGIC {
		log.Printf("Bad magic: %x", hdrBytes[0])
		runtime.Goexit()
	}
	rv.OpCode = gomemcached.CommandCode(hdrBytes[1])
	rv.Key = make([]byte, bigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	rv.Status = uint16(hdrBytes[7])
	bodyLen := bigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	//rv.Opaque = ReadUint32(hdrBytes, 12)
	rv.Cas = bigEndian.Uint64(hdrBytes[16:])
	return
}
