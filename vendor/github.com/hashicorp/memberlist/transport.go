package memberlist

import (
	"net"
	"time"
)

// Packet is used to provide some metadata about incoming packets from peers
// over a packet connection, as well as the packet payload.
// Packet用于提供从peers来的packets的元数据信息，以及packet负载
type Packet struct {
	// Buf has the raw contents of the packet.
	// Buf有着packet的raw contents
	Buf []byte

	// From has the address of the peer. This is an actual net.Addr so we
	// can expose some concrete details about incoming packets.
	From net.Addr

	// Timestamp is the time when the packet was received. This should be
	// taken as close as possible to the actual receipt time to help make an
	// accurate RTT measurement during probes.
	Timestamp time.Time
}

// Transport is used to abstract over communicating with other peers. The packet
// interface is assumed to be best-effort and the stream interface is assumed to
// be reliable.
// Transport用于抽象和其他peers之间的通信，packet接口假设是best-effort的，而stream接口被
// 认为是可靠的
type Transport interface {
	// FinalAdvertiseAddr is given the user's configured values (which
	// might be empty) and returns the desired IP and port to advertise to
	// the rest of the cluster.
	// FinalAdvertiseAddr是给定用户的configured values，返回desired IP以及port用来
	// 给cluster的其他部分
	FinalAdvertiseAddr(ip string, port int) (net.IP, int, error)

	// WriteTo is a packet-oriented interface that fires off the given
	// payload to the given address in a connectionless fashion. This should
	// return a time stamp that's as close as possible to when the packet
	// was transmitted to help make accurate RTT measurements during probes.
	// WriteTo是一个面向packet的接口，以一种无连接的方式发送给定的负载到给定的地址
	//
	// This is similar to net.PacketConn, though we didn't want to expose
	// that full set of required methods to keep assumptions about the
	// underlying plumbing to a minimum. We also treat the address here as a
	// string, similar to Dial, so it's network neutral, so this usually is
	// in the form of "host:port".
	WriteTo(b []byte, addr string) (time.Time, error)

	// PacketCh returns a channel that can be read to receive incoming
	// packets from other peers. How this is set up for listening is left as
	// an exercise for the concrete transport implementations.
	// PacketCh返回一个channel可以用来从其他peers读取incoming packets
	PacketCh() <-chan *Packet

	// DialTimeout is used to create a connection that allows us to perform
	// two-way communication with a peer. This is generally more expensive
	// than packet connections so is used for more infrequent operations
	// such as anti-entropy or fallback probes if the packet-oriented probe
	// failed.
	// DialTimeout用来建立一个连接，它允许我们能够和一个peer进行双向的交互，这通常比
	// packet connections更昂贵，因此用得也更少，一般用于anti-entropy或者fallback probes
	DialTimeout(addr string, timeout time.Duration) (net.Conn, error)

	// StreamCh returns a channel that can be read to handle incoming stream
	// connections from other peers. How this is set up for listening is
	// left as an exercise for the concrete transport implementations.
	// StreamCh返回一个channel，可以用来读取并且处理来自其他peers的stream connections
	StreamCh() <-chan net.Conn

	// Shutdown is called when memberlist is shutting down; this gives the
	// transport a chance to clean up any listeners.
	// Shutdown会在memberlist被关闭的时候被调用，者给了transport一个机会用来清除任何的listeners
	Shutdown() error
}
