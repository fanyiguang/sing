package bufio

import (
	"context"
	"errors"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/rw"
	"github.com/sagernet/sing/common/task"
	"io"
	"net"
	"syscall"
)

func CopyEx(destination io.Writer, source io.Reader, clock *AlarmClock) (n int64, err error) {
	if source == nil {
		return 0, E.New("nil reader")
	} else if destination == nil {
		return 0, E.New("nil writer")
	}
	originSource := source
	var readCounters, writeCounters []N.CountFunc
	for {
		source, readCounters = N.UnwrapCountReader(source, readCounters)
		destination, writeCounters = N.UnwrapCountWriter(destination, writeCounters)
		if cachedSrc, isCached := source.(N.CachedReader); isCached {
			cachedBuffer := cachedSrc.ReadCached()
			if cachedBuffer != nil {
				if !cachedBuffer.IsEmpty() {
					_, err = destination.Write(cachedBuffer.Bytes())
					if err != nil {
						cachedBuffer.Release()
						return
					}
				}
				cachedBuffer.Release()
				continue
			}
		}
		srcSyscallConn, srcIsSyscall := source.(syscall.Conn)
		dstSyscallConn, dstIsSyscall := destination.(syscall.Conn)
		if srcIsSyscall && dstIsSyscall {
			var handled bool
			handled, n, err = copyDirectEx(srcSyscallConn, dstSyscallConn, readCounters, writeCounters, clock)
			if handled {
				return
			}
		}
		break
	}
	return CopyExtendedEx(originSource, NewExtendedWriter(destination), NewExtendedReader(source), readCounters, writeCounters, clock)
}

func CopyExtendedEx(originSource io.Reader, destination N.ExtendedWriter, source N.ExtendedReader, readCounters []N.CountFunc, writeCounters []N.CountFunc, clock *AlarmClock) (n int64, err error) {
	frontHeadroom := N.CalculateFrontHeadroom(destination)
	rearHeadroom := N.CalculateRearHeadroom(destination)
	readWaiter, isReadWaiter := CreateReadWaiter(source)
	if isReadWaiter {
		needCopy := readWaiter.InitializeReadWaiter(N.ReadWaitOptions{
			FrontHeadroom: frontHeadroom,
			RearHeadroom:  rearHeadroom,
			MTU:           N.CalculateMTU(source, destination),
		})
		if !needCopy || common.LowMemory {
			var handled bool
			handled, n, err = copyWaitWithPoolEx(originSource, destination, readWaiter, readCounters, writeCounters, clock)
			if handled {
				return
			}
		}
	}
	return CopyExtendedWithPoolEx(originSource, destination, source, readCounters, writeCounters, clock)
}

func CopyExtendedBufferEx(originSource io.Writer, destination N.ExtendedWriter, source N.ExtendedReader, buffer *buf.Buffer, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	buffer.IncRef()
	defer buffer.DecRef()
	frontHeadroom := N.CalculateFrontHeadroom(destination)
	rearHeadroom := N.CalculateRearHeadroom(destination)
	buffer.Resize(frontHeadroom, 0)
	buffer.Reserve(rearHeadroom)
	var notFirstTime bool
	for {
		err = source.ReadBuffer(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		buffer.OverCap(rearHeadroom)
		err = destination.WriteBuffer(buffer)
		if err != nil {
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func CopyExtendedWithPoolEx(originSource io.Reader, destination N.ExtendedWriter, source N.ExtendedReader, readCounters []N.CountFunc, writeCounters []N.CountFunc, clock *AlarmClock) (n int64, err error) {
	frontHeadroom := N.CalculateFrontHeadroom(destination)
	rearHeadroom := N.CalculateRearHeadroom(destination)
	bufferSize := N.CalculateMTU(source, destination)
	if bufferSize > 0 {
		bufferSize += frontHeadroom + rearHeadroom
	} else {
		bufferSize = buf.BufferSize
	}
	var notFirstTime bool
	for {
		buffer := buf.NewSize(bufferSize)
		buffer.Resize(frontHeadroom, 0)
		buffer.Reserve(rearHeadroom)
		err = source.ReadBuffer(buffer)
		if err != nil {
			buffer.Release()
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		if dataLen > 0 {
			clock.ResetTimer()
		}
		buffer.OverCap(rearHeadroom)
		err = destination.WriteBuffer(buffer)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func CopyConnEx(ctx context.Context, source net.Conn, destination net.Conn) error {
	return CopyConnContextListEx([]context.Context{ctx}, source, destination)
}

func CopyConnContextListEx(contextList []context.Context, source net.Conn, destination net.Conn) error {
	var group task.Group
	clock := NewAlarmClock(func() {
		_ = common.Close(source, destination)
	})
	defer clock.Close()
	if _, dstDuplex := common.Cast[rw.WriteCloser](destination); dstDuplex {
		group.Append("upload", func(ctx context.Context) error {
			err := common.Error(CopyEx(destination, source, clock))
			if err == nil {
				rw.CloseWrite(destination)
			} else {
				common.Close(destination)
			}
			clock.Start(60)
			return err
		})
	} else {
		group.Append("upload", func(ctx context.Context) error {
			defer func() {
				common.Close(destination)
				clock.Start(60)
			}()
			return common.Error(CopyEx(destination, source, clock))
		})
	}
	if _, srcDuplex := common.Cast[rw.WriteCloser](source); srcDuplex {
		group.Append("download", func(ctx context.Context) error {
			err := common.Error(CopyEx(source, destination, clock))
			if err == nil {
				rw.CloseWrite(source)
			} else {
				common.Close(source)
			}
			clock.Start(60)
			return err
		})
	} else {
		group.Append("download", func(ctx context.Context) error {
			defer func() {
				common.Close(source)
				clock.Start(60)
			}()
			return common.Error(CopyEx(source, destination, clock))
		})
	}
	group.Cleanup(func() {
		common.Close(source, destination)
	})
	return group.RunContextList(contextList)
}

func CopyPacketEx(destinationConn N.PacketWriter, source N.PacketReader) (n int64, err error) {
	var readCounters, writeCounters []N.CountFunc
	var cachedPackets []*N.PacketBuffer
	originSource := source
	for {
		source, readCounters = N.UnwrapCountPacketReader(source, readCounters)
		destinationConn, writeCounters = N.UnwrapCountPacketWriter(destinationConn, writeCounters)
		if cachedReader, isCached := source.(N.CachedPacketReader); isCached {
			packet := cachedReader.ReadCachedPacket()
			if packet != nil {
				cachedPackets = append(cachedPackets, packet)
				continue
			}
		}
		break
	}
	if cachedPackets != nil {
		n, err = WritePacketWithPoolEx(originSource, destinationConn, cachedPackets)
		if err != nil {
			return
		}
	}
	frontHeadroom := N.CalculateFrontHeadroom(destinationConn)
	rearHeadroom := N.CalculateRearHeadroom(destinationConn)
	var (
		handled bool
		copeN   int64
	)
	readWaiter, isReadWaiter := CreatePacketReadWaiter(source)
	if isReadWaiter {
		needCopy := readWaiter.InitializeReadWaiter(N.ReadWaitOptions{
			FrontHeadroom: frontHeadroom,
			RearHeadroom:  rearHeadroom,
			MTU:           N.CalculateMTU(source, destinationConn),
		})
		if !needCopy || common.LowMemory {
			handled, copeN, err = copyPacketWaitWithPool(originSource, destinationConn, readWaiter, readCounters, writeCounters, n > 0)
			if handled {
				n += copeN
				return
			}
		}
	}
	copeN, err = CopyPacketWithPoolEx(originSource, destinationConn, source, readCounters, writeCounters, n > 0)
	n += copeN
	return
}

func CopyPacketWithPoolEx(originSource N.PacketReader, destinationConn N.PacketWriter, source N.PacketReader, readCounters []N.CountFunc, writeCounters []N.CountFunc, notFirstTime bool) (n int64, err error) {
	frontHeadroom := N.CalculateFrontHeadroom(destinationConn)
	rearHeadroom := N.CalculateRearHeadroom(destinationConn)
	bufferSize := N.CalculateMTU(source, destinationConn)
	if bufferSize > 0 {
		bufferSize += frontHeadroom + rearHeadroom
	} else {
		bufferSize = buf.UDPBufferSize
	}
	var destination M.Socksaddr
	for {
		buffer := buf.NewSize(bufferSize)
		buffer.Resize(frontHeadroom, 0)
		buffer.Reserve(rearHeadroom)
		destination, err = source.ReadPacket(buffer)
		if err != nil {
			buffer.Release()
			return
		}
		dataLen := buffer.Len()
		buffer.OverCap(rearHeadroom)
		err = destinationConn.WritePacket(buffer, destination)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func WritePacketWithPoolEx(originSource N.PacketReader, destinationConn N.PacketWriter, packetBuffers []*N.PacketBuffer) (n int64, err error) {
	frontHeadroom := N.CalculateFrontHeadroom(destinationConn)
	rearHeadroom := N.CalculateRearHeadroom(destinationConn)
	var notFirstTime bool
	for _, packetBuffer := range packetBuffers {
		buffer := buf.NewPacket()
		buffer.Resize(frontHeadroom, 0)
		buffer.Reserve(rearHeadroom)
		_, err = buffer.Write(packetBuffer.Buffer.Bytes())
		packetBuffer.Buffer.Release()
		if err != nil {
			buffer.Release()
			continue
		}
		dataLen := buffer.Len()
		buffer.OverCap(rearHeadroom)
		err = destinationConn.WritePacket(buffer, packetBuffer.Destination)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
	}
	return
}

func CopyPacketConnEx(ctx context.Context, source N.PacketConn, destination N.PacketConn) error {
	return CopyPacketConnContextListEx([]context.Context{ctx}, source, destination)
}

func CopyPacketConnContextListEx(contextList []context.Context, source N.PacketConn, destination N.PacketConn) error {
	var group task.Group
	group.Append("upload", func(ctx context.Context) error {
		return common.Error(CopyPacketEx(destination, source))
	})
	group.Append("download", func(ctx context.Context) error {
		return common.Error(CopyPacketEx(source, destination))
	})
	group.Cleanup(func() {
		common.Close(source, destination)
	})
	group.FastFail()
	return group.RunContextList(contextList)
}

func copyWaitWithPoolEx(originSource io.Reader, destination N.ExtendedWriter, source N.ReadWaiter, readCounters []N.CountFunc, writeCounters []N.CountFunc, clock *AlarmClock) (handled bool, n int64, err error) {
	handled = true
	var (
		buffer       *buf.Buffer
		notFirstTime bool
	)
	for {
		buffer, err = source.WaitReadBuffer()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		if dataLen > 0 {
			clock.ResetTimer()
		}
		err = destination.WriteBuffer(buffer)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func copyDirectEx(source syscall.Conn, destination syscall.Conn, readCounters []N.CountFunc, writeCounters []N.CountFunc, clock *AlarmClock) (handed bool, n int64, err error) {
	rawSource, err := source.SyscallConn()
	if err != nil {
		return
	}
	rawDestination, err := destination.SyscallConn()
	if err != nil {
		return
	}
	handed, n, err = spliceEx(rawSource, rawDestination, readCounters, writeCounters, clock)
	return
}
