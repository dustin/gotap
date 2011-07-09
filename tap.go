package tap

import . "./mc_constants"
import . "./byte_manipulation"

import (
        "net"
        "log"
	"fmt"
        "io"
        "bufio"
        "runtime"
        )

type TapOperation struct {
	OpCode            uint8
	Status            uint16
	Cas               uint64
	Extras, Key, Body []byte
}

func (op *TapOperation) ToString() (rv string) {
	var types string
	switch op.OpCode {
        case TAP_CONNECT: types = "CONNECT"; break;
        case TAP_MUTATION: types = "MUTATION"; break;
        case TAP_DELETE: types = "DELETE"; break;
        case TAP_FLUSH: types = "FLUSH"; break;
        case TAP_OPAQUE: types = "OPAQUE"; break;
        case TAP_VBUCKET_SET: types = "VBUCKET_SET"; break;
        case TAP_CHECKPOINT_START: types = "CHECKPOINT_START"; break;
	case TAP_CHECKPOINT_END: types = "CHECKPOINT_END"; break;
	default:
		types = fmt.Sprintf("<unknown 0x%x>", op.OpCode)
        }

	rv = fmt.Sprintf("<TapOperation %s>", types)

	return rv
}

type TapClient struct {
        Conn net.Conn
        writer *bufio.Writer
}

type TapArguments struct {
	Backfill uint64
	Dump bool
	VBuckets []uint16
	Takeover bool
	SupportAck bool
	KeysOnly bool
	Checkpoint bool
	ClientName string
}

func (client *TapClient) Feed(ch chan TapOperation) {
	for {
		ch <- getResponse(client)
	}
}

func transmitRequest(o *bufio.Writer, req MCRequest) {
        // 0
        writeByte(o, REQ_MAGIC)
        writeByte(o, req.Opcode)
        writeUint16(o, uint16(len(req.Key)))
        // 4
        writeByte(o, uint8(len(req.Extras)))
        writeByte(o, 0)
        writeUint16(o, req.VBucket)
        // 8
        writeUint32(o, uint32(len(req.Body))+
                uint32(len(req.Key))+
                uint32(len(req.Extras)))
        // 12
        writeUint32(o, req.Opaque)
        // 16
        writeUint64(o, req.Cas)
        // The rest
        writeBytes(o, req.Extras)
        writeBytes(o, req.Key)
        writeBytes(o, req.Body)
        o.Flush()
}

func start(client *TapClient, args TapArguments) {
        var req MCRequest
	empty := make([]byte, 0)
        req.Opcode = TAP_CONNECT
        req.Key = empty
        req.Cas = 0
        req.Opaque = 0
        req.Extras = WriteUint64(BACKFILL)
        req.Body = empty
        transmitRequest(client.writer, req)
}

func Connect(prot string, dest string, args TapArguments) (rv *TapClient) {
        conn, err := net.Dial(prot, dest)
        if err != nil {
                log.Fatalf("Failed to connect: %s", err)
        }
        rv = new(TapClient)
        rv.Conn = conn
        rv.writer, err = bufio.NewWriterSize(rv.Conn, 256)
        if err != nil {
                panic("Can't make a buffer")
        }

	start(rv, args)

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

func writeByte(s *bufio.Writer, b byte) {
        var data []byte = make([]byte, 1)
        data[0] = b
        writeBytes(s, data)
}

func writeUint16(s *bufio.Writer, n uint16) {
        data := WriteUint16(n)
        writeBytes(s, data)
}

func writeUint32(s *bufio.Writer, n uint32) {
        data := WriteUint32(n)
        writeBytes(s, data)
}

func writeUint64(s *bufio.Writer, n uint64) {
        data := WriteUint64(n)
        writeBytes(s, data)
}

func readOb(s net.Conn, buf []byte) {
        x, err := io.ReadFull(s, buf)
        if err != nil || x != len(buf) {
                log.Printf("Error reading part: %s", err)
                runtime.Goexit()
        }
}

func getResponse(client *TapClient) TapOperation {
        hdrBytes := make([]byte, HDR_LEN)
        bytesRead, err := io.ReadFull(client.Conn, hdrBytes)
        if err != nil || bytesRead != HDR_LEN {
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
        if hdrBytes[0] != REQ_MAGIC {
                log.Printf("Bad magic: %x", hdrBytes[0])
                runtime.Goexit()
        }
        rv.OpCode = hdrBytes[1]
        rv.Key = make([]byte, ReadUint16(hdrBytes, 2))
        rv.Extras = make([]byte, hdrBytes[4])
        rv.Status = uint16(hdrBytes[7])
        bodyLen := ReadUint32(hdrBytes, 8) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
        rv.Body = make([]byte, bodyLen)
        // rv.Opaque = ReadUint32(hdrBytes, 12)
        rv.Cas = ReadUint64(hdrBytes, 16)
        return
}
