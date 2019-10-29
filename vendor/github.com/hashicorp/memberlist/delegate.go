package memberlist

// Delegate is the interface that clients must implement if they want to hook
// into the gossip layer of Memberlist. All the methods must be thread-safe,
// as they can and generally will be called concurrently.
// Delegate是clients必须实现的接口，如果它们想要接入Memberlist的gossip layer
type Delegate interface {
	// NodeMeta is used to retrieve meta-data about the current node
	// when broadcasting an alive message. It's length is limited to
	// the given byte size. This metadata is available in the Node structure.
	// NodeMeta用于在广播一个alive message的时候获取当前节点的元数据
	NodeMeta(limit int) []byte

	// NotifyMsg is called when a user-data message is received.
	// Care should be taken that this method does not block, since doing
	// so would block the entire UDP packet receive loop. Additionally, the byte
	// slice may be modified after the call returns, so it should be copied if needed
	// NotifyMsg会在接收到用户数据的message之后被调用，需要注意的是这个方法不能阻塞
	// 因为这会阻塞整个的UDP packet receive loop，另外，byte slice也可能在调用返回之后被修改
	// 因此有必要的话，先拷贝
	NotifyMsg([]byte)

	// GetBroadcasts is called when user data messages can be broadcast.
	// It can return a list of buffers to send. Each buffer should assume an
	// overhead as provided with a limit on the total byte size allowed.
	// The total byte size of the resulting data to send must not exceed
	// the limit. Care should be taken that this method does not block,
	// since doing so would block the entire UDP packet receive loop.
	// GetBroadcasts会在user data message可以被广播的时候被调用，它可以返回一系列的用于发送的buffer
	// 每个buffer应该假设一个overhead，因为提供一个limit作为发送的总大小是被允许的
	// 发送的最终数据的总大小不能超过limit，这个函数同样不能阻塞，因为这会阻塞整个UDP packet receive loop
	GetBroadcasts(overhead, limit int) [][]byte

	// LocalState is used for a TCP Push/Pull. This is sent to
	// the remote side in addition to the membership information. Any
	// data can be sent here. See MergeRemoteState as well. The `join`
	// boolean indicates this is for a join instead of a push/pull.
	// LocalState用于一个TCP Push/Pull，这个用来在membership information之外
	// 发送额外的信息，任何数据都能在这发送，join这个布尔值表示这用于join而不是一个push/pull
	LocalState(join bool) []byte

	// MergeRemoteState is invoked after a TCP Push/Pull. This is the
	// state received from the remote side and is the result of the
	// remote side's LocalState call. The 'join'
	// boolean indicates this is for a join instead of a push/pull.
	// MergeRemoteState在一个TCP Push/Pull之后被调用，这是从remote side获取到的
	// state并且是remote side的LocalState调用的结果，同样，join表示这是一个join而不是push/pull
	MergeRemoteState(buf []byte, join bool)
}
