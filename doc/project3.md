# **Project3 MultiRaftKV**

在这个项目中，将实现带有平衡调度器 (balance scheduler) 的 multi raft-based 的 kv 服务器。该服务器就集群由多个 raft groups 组成，每个 raft group 负责一个单独的键范围。

对单个区域的请求和以前一样处理，但多个区域可以同时处理请求，这提高了性能，但也带来了一些新的挑战，例如平衡每个区域的请求等，该项目有3个部分，包括：

1. 实现成员变更（membership change）和领导变更（leadership change）
2. 在 raftstore 上实现 **conf 更改**和**区域拆分**（region split）
3. 引入调度器

## Part A

实现成员变更（membership change）和领导变更（leadership change），Membership change 也即是 conf change，用于在raft group中添加或移除 peer，可以改变 raft group 的 quorum。Leadership change 也即是 leader transfer，用于将领导权转移给另一个peer，这对于平衡非常有用。

### 3A  Code

需要修改的代码位于 `raft/raft.go` and `raft/rawnode.go`，另请参阅`proto/proto/eraft.proto`查看需要处理的新消息。And both conf change and leader transfer **are triggered by the upper application**, so you may want to start at `raft/rawnode.go`.

```go
type ConfChangeType int32
const (
	ConfChangeType_AddNode    ConfChangeType = 0
	ConfChangeType_RemoveNode ConfChangeType = 1
)

func (r *SchedulerTaskHandler) onRegionHeartbeatResponse(resp *schedulerpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch,
                           resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerRequest{
				ChangeType: changePeer.ChangeType,
				Peer:       changePeer.Peer,
			},
		}, message.NewCallback())
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch,
                           resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: transferLeader.Peer,
			},
		}, message.NewCallback())
	}
}
```

### 实现 leader transfer

为了实现领导者转移，引入两种新消息类型: `MsgTransferLeader` 以及 `MsgTimeoutNow`. 

当需要进行领导权的转移时，首先将类型为 `MsgTransferLeader` 的信息传递给 leader 的 `raft.Raft.Step` 方法进行处理，该方法被 `raft/rawnode.go`中的 `TransferLeader` 方法显式调用。

```go
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{
        MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
```

为确保传输的成功，当前的 leader 应该确认转移目标的资质，如是否保存了最新的日志信息（**需要检查转移对象是否存在**）等。如果转移目标不符合转移条件，就需要帮助转移目标，使得其最终符合转移资质。如果转移目标的日志不是最新的，那么将**发送 `MsgAppend` message 更新日志信息，并停止接收新的日志**。

停止接收日志，使用 `leadTransferee` 进行控制，当每次结点成为 leader 时，将其置为0表示没有转移对象；当收到对应的转移信息后，将其置为转移对象的id，然后用此量控制是否接收 `MessageType_MsgPropose` 进行日志的处理。

在接收到 appendresponse 后判断是否可以进行转移，如不行则继续进行日志的更新；为防止某些命名在网络中丢失导致转移失败，通过心跳的方式定期检查是否可以转移，直到转移成功。

当转移对象符合转移条件后，leader 应该发送 `MsgTimeoutNow` message 到转移目标，当转移目标接收到此信号后，应该立即开始进行新的选举。

### 实现 conf change 配置变迁

本次实现的 Conf change 算法只能一个一个地添加或删除peer，需要注意的是conf change 起始于调用 leader 的 `raft.RawNode.ProposeConfChange` 方法，此方法会提交类型 `pb.Entry.EntryType` 为 `EntryConfChange` 的日志，日志的 `pb.Entry.Data` 存放 `pb.ConfChange` 的信息。 

当类型为 `EntryConfChange` 的日志被提交后，需要通过 `RawNode.ApplyConfChange` 方法进行应用，该过程在`peer_msg_handler.go` 中 `HandleRaftReady` 方法中，对于 `ready.CommittedEntries` 的 process 中。

这样之后，你就可以根据 `pb.ConfChange` 信息，通过 `raft.Raft.addNode` 以及 `raft.Raft.removeNode` 方法修改结点。

> Hints:
>
> - 可以将 `MsgTransferLeader` message 的 `Message.from` 设置为转移目标id
> - 将 `MsgHup` message 传递给 `Raft.Step` 方法可以开始选举，也可以直接调用选举方法进行选举
> - 调用 `pb.ConfChange.Marshal` 将 `pb.ConfChange` 转换成字节流，并将之存放进 `pb.Entry.Data` 中进行传递。
> - 注意修改`PendingConfIndex`，在上一次 conf change 为完全生效时，下一次的 conf change 不得起效。该值将被设置为上一次 conf change 日志的序号，每次进行 conf change 日志 propose 时，只有当 applied index 小于等于 该日志序号才行，**否则先拒绝处理该请求，将msg重新放入msgs数组中**。
> - 移除节点后，**某些日志可能可以提交了，因此需要在移除节点方法中增加检验 commited 的部分**



### 测试问题与解决方案：

1. 测试函数 `TestLeaderTransferToUpToDateNodeFromFollower3A` 非常奇怪，他**将`MsgTransferLeader` 信息发送给了非leader**，这种行为非常迷惑，为了通过测试，必须做出一些奇怪操作，如将此信息转发给leader结点，但是这样就**违背了`MsgTransferLeader` message 是本地信息不源自网络的要求**，总之非常迷惑。

   ```
   nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgTransferLeader})
   ```

2. 测试函数 `TestTransferNonMember3A` 其作用是测试向已经从集群中移除的结点发送信息后，应该什么都不发生，但是这个测试函数比较诡异。`r.Prs` 中仅删除了 1，我认为既然结点从集群中删除，对于删除的结点来说，`r.Prs` 应该直接置为空即可。

   ```go
   r := newTestRaft(1, []uint64{2, 3, 4}, 5, 1, NewMemoryStorage())
   r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTimeoutNow})
   r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse})
   r.Step(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse})
   ```

## Part B

在完成 Part A 后，Raft module 支持成员的增加与移除以及 leadership 转移，这部分需要在A部分的基础上让 TinyKV 支持定义在 `proto/proto/raft_cmdpb.proto` 中的 admin command，命令分为四类：

- **CompactLog** 
- **TransferLeader**
- **ChangePeer**
- **Split**

`TransferLeader` 以及 `ChangePeer` 是基于 Raft 的与领导变更和成员变更相关的命令。这些命令将作为基本的操作步骤，用于后续进行平衡调度过程中。 

`Split` 将一个 Region 分裂为两个 Region，这是 multi raft 的基础。

### **3B** Code

需要修改的代码位于 `kv/raftstore/peer_msg_handler.go` 以及 `kv/raftstore/peer.go` 中。

### 提交 transfer leader 命令

作为一种 raft 命令，`TransferLeader` 本来应该作为 raft 日志被提交到 raft 模块中，但是 `TransferLeader` 本质上是本地信息，不需要提交到其他结点，因此只需要**调用 `RawNode` 的方法 `TransferLeader()` 代替原先的 `Propose()`** 即可。

同时，使用 `cb.Done` 将结果传回。

### 在 raftstore 中实现 conf change

conf change 有两种不同的类型， 分别是 `AddNode` 以及 `RemoveNode`，为在 raftstore  实现这两种功能，需要首先了解  **`RegionEpoch`**  的作用。`RegionEpoch` 是 `metapb.Region` 元数据的一部分，当一个 Region 增加或移除 Peer 或进行 Region 分裂时，Region 的 epoch 发生了变化。

**当进行 `ConfChange`  时，`RegionEpoch` 的 `conf_ver` 进行自增；当进行 Region 分裂或合并（split/merge）时，`version`进行自增。这两个 version 信息主要用于，在网络隔离时出现一个 Region 同时存在两个 leader 的情况下，确保 region 信息的是最新的。**

您需要让 raftstore 支持处理 `conf change` 命令. 主要流程如下所示:

1.  通过 `ProposeConfChange` 提交 conf change admin command
2. 在日志被提交后，修改 `RegionLocalState`，包括`RegionEpoch` 以及 `Peers` in `Region`
3. 调用 `ApplyConfChange()` of `raft.RawNode`
4. callback

> Hints:
>
> - 由于需要更新 Region 信息，将 Peer 对应的信息添加/移除到本地 Peer 的 Region 中，但是由于 `EntryType_EntryConfChange` 的日志信息中不包含 `storeID`，无法将 Peer 添加到本地，因此需要使用 `Context` 字段传递该信息，传递方式与 request 的传递方式类似：
>
>   ```go
>   context, _ := msg.Marshal()
>   cc := eraftpb.ConfChange{
>       ChangeType:           req.ChangePeer.ChangeType,
>       NodeId:               req.ChangePeer.Peer.Id,
>       Context:              context,
>   }
>   
>   msg := &raft_cmdpb.RaftCmdRequest{}
>   err = msg.Unmarshal(cc.Context)
>   ```
>
> - 为了执行 `AddNode`, 新添加的 Peer 是通过 leader 的 heartbeat  创造的，具体的过程请参见 `storeWorker` 的 `maybeCreatePeer()` 方法。在 Peer 刚创建时是未初始化的，而且我们都不知道它 Region 的任何信息。因此，我们用0来初始化它的 `Log Term ` 和 `Index`，这样 leader 就知道这个Follower没有数据（存在0到5的Log间隔），它会直接向这个Follower发送快照。
>
> - 为了执行  `RemoveNode`，应该显式调用方法 `destroyPeer()`来停止 Raft 模块。
>
> - 别忘了更新区域状态（region state），该状态位于`GlobalContext` 的 `storeMeta` 中
>
> - 在将 Peer 加入或移除时，需要修改peerCache，以防止出错
>
>   ```go
>   d.insertPeerCache(peer)
>   d.removePeerCache(cc.NodeId)
>   ```
>
> - 测试代码多次调度一个 conf 更改的命令，直到 conf 更改被应用，因此您需要考虑如何忽略同一个 conf 更改的重复命令（Test code schedules the command of one conf change multiple times until the conf change is applied, so you need to consider how to ignore the duplicate commands of the same conf change）。 
>
> - raft 模块中 HeartBeat 的 committed应该为 初始值 util.RaftInvalidIndex，这样才能创建 Peer ，详见 `maybeCreatePeer()` 方法
>
> - 特别需要注意的是，由于 Peerdestroy时会删除 BTree上的结点，因此需要修改 maybeCreatePeer，在最后加上ReplaceOrInsert。
>
>   ```go
>   if isInitialized && 
>   	meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
>   		panic(d.Tag + " meta corruption detected")
>   	}
>       
>   func (d *storeWorker) maybeCreatePeer(
>       regionID uint64, msg *rspb.RaftMessage) (bool, error) {
>   	......
>   	meta.regionRanges.ReplaceOrInsert(&regionItem{region: peer.Region()})
>   	return true, nil
>   }
>   ```
>
>   

### 在 raftstore 中实现 split region

[![raft_group](https://github.com/tidb-incubator/tinykv/raw/course/doc/imgs/keyspace.png)](https://github.com/tidb-incubator/tinykv/blob/course/doc/imgs/keyspace.png)

为支持 multi-raft，系统将数据分片，让每个 Raft group 只存储部分数据。

Hash 和 Range 通常用于数据分片，TinyKV 使用 Range（顺序切片）作为分片手段，主要原因是 Range 可以将具有相同前缀的 key 连续存放，方便 scan 等操作进行；此外 Range 在 split 方面的表现也优于 Hash，**通常只涉及元数据修改，不需要移动数据**。

```go
message Region {
 uint64 id = 1;
 bytes start_key = 2;  // Region key range [start_key, end_key).
 bytes end_key = 3;
 RegionEpoch region_epoch = 4;
 repeated Peer peers = 5
}

message RegionEpoch {
    optional uint64 conf_ver    = 1 [(gogoproto.nullable) = false];
    optional uint64 version     = 2 [(gogoproto.nullable) = false];
}

message Peer {
    optional uint64 id          = 1 [(gogoproto.nullable) = false];
    optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
}
```

让我们重新看一下 Region 的定义，它包括两个字段 `start_key ` 以及 `end_key` ，这两个字段用于指示 Region 的数据范围。所以 split 是支持 multi-raft 的关键一步。

> **id**：Region 的唯一表示，通过 PD 全局唯一分配。
>
> **start_key, end_key**：用来表示这个 Region 的范围 [start_key, end_key)，对于最开始的 region，start 和 end key 都是空，TiKV 内部会特殊处理。
>
> **region_epoch**：当一个 Region 添加或者删除 Peer，或者 split 等，我们就会认为这个 Region 的 epoch 发生的变化，RegionEpoch 的 conf_ver 会在每次做 ConfChange 的时候递增，而 version 则是会在每次做 split/merge 的时候递增。
>
> **peers**：当前 Region 包含的节点信息。对于一个 Raft Group，我们通常有三个副本，每个副本我们使用 Peer 来表示，Peer 的 id 也是全局由 PD 分配，而 store_id 则表明这个 Peer 在哪一个 Store 上面。

一开始，只有一个 Region 的范围是 [“”, “”)。您可以将键空间视为一个循环，因此 [“”, “”) 代表整个空间。写入数据后，split checker会每间隔`cfg.SplitRegionCheckTickInterval`时间定期使用检查每个区域的大小，如果可能的话，生成一个 split key 将 Region 分成两部分，可以在`kv/raftstore/runner/split_check.go` 中查看逻辑 。split key 将被包装为`MsgSplitRegion`最后交给`onPrepareSplitRegion()` 进行处理。

```go
	msg := message.Msg{
		Type:     message.MsgTypeSplitRegion,
		RegionID: regionId,
		Data: &message.MsgSplitRegion{
			RegionEpoch: region.GetRegionEpoch(),
			SplitKey:    key,
		},
	}
	err = r.router.Send(regionId, msg)

func (d *peerMsgHandler) onPrepareSplitRegion(
    regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (r *SchedulerTaskHandler) Handle(t worker.Task) {
	switch t.(type) {
	case *SchedulerAskSplitTask:
		r.onAskSplit(t.(*SchedulerAskSplitTask))
	......
	}
}
```

为了确保新创建的 Region 和 Peers 的 id 是唯一的，这些 id 由调度器 scheduler 分配。 `onPrepareSplitRegion()`实际上为 pd worker 安排了一项任务，向调度程序询问 id，同时接收来自调度响应后生成 split admin 命令，详细见`kv/raftstore/runner/scheduler_task.go` 中的 `onAskSplit()` 方法。

```go
func (r *SchedulerTaskHandler) onAskSplit(t *SchedulerAskSplitTask) {
	......
	aq := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitRequest{
			SplitKey:    t.SplitKey,
			NewRegionId: resp.NewRegionId,
			NewPeerIds:  resp.NewPeerIds,
		},
	}
	r.sendAdminRequest(t.Region.GetId(), t.Region.GetRegionEpoch(),
                       t.Peer, aq, t.Callback)
}
```

所以你的任务是实现处理 split admin 命令的过程，就像 conf change 一样。提供的框架支持 multiple raft，见`kv/raftstore/router.go`。

当一个 Region 分裂成两个 Region 时，其中一个 Region 会继承分裂前的元数据，只修改它的 Range 和 RegionEpoch，另一个 Region 会创建相关的元信息，创建流程如下：

1. 进行状态检查，`CheckKeyInRegion`、`CheckRegionEpoch` 以及 `CheckRegion` ，如果存在异常，callback 返回 ErrResp 。`CheckKeyInRegion` 必须要检查，而 `CheckRegionEpoch` 以及 `CheckRegion` 主要是为了防止**重复的 split 命令导致出错**。

2. 参考 `kv/raftstore/raftstore.go` 中的方法 `loadPeers` 以及 `start` ，观察 peer 的创建过程，主要分为如下几步：首先，调用 `createPeer` 方法创建 Peer，使用 `ReplaceOrInsert` 将新的 Region 插入 BTree中；然后，将新的 Region 加入 `ctx.storeMeta.regions[]`；最后，使用 `bs.router.register(peer)`将其登记到系统中，以及 `router.send(regionID, message.Msg{RegionID: regionID, Type: message.MsgTypeStart})` 将系统启动。

   ```go
   peer, err := createPeer(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
   
   ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
   ctx.storeMeta.regions[regionID] = region
   			
   bs.router.register(peer)
   ```


> Hints:
>
> - 这个新创建的 Region 的对应 Peer 应该由 `createPeer()`（在 `peer.go` 中） router.regions 创建并注册到 router.regions。并且应该`regionRanges`在 ctx.StoreMeta 中插入区域信息（The corresponding Peer of this newly-created Region should be created by `createPeer()` and registered to the router.regions. And the region’s info should be inserted into `regionRanges` in ctx.StoreMeta.）
>
>   参考 `loadPeers()` ：
>
>   ```go
>   func (bs *Raftstore) loadPeers() ([]*peer, error) {
>   	......
>   	peer,err:=createPeer(storeID,ctx.cfg,ctx.regionTaskSender,ctx.engine,region)
>   	ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
>   	ctx.storeMeta.regions[regionID] = region
>       ......
>   	kvWB.MustWriteToDB(ctx.engine.Kv)
>   	raftWB.MustWriteToDB(ctx.engine.Raft)
>       ......
>   }
>   for _, peer := range regionPeers {
>   	bs.router.register(peer)
>   }
>      



> #### Placement Driver(PD)相关知识
>
> **bootstrap_cluster**：当我们启动一个 TiKV 服务的时候，首先需要通过 is_cluster_bootstrapped 来判断整个 TiKV 集群是否已经初始化，如果还没有初始化，我们就会在该 TiKV 服务上面创建第一个 region。
>
> **region_heartbeat**：定期 Region 向 PD 汇报自己的相关信息，供 PD 做后续的调度。譬如，如果一个 Region 给 PD 上报的 peers 的数量小于预设的副本数，那么 PD 就会给这个 Region 添加一个新的副本 Peer。
>
> **store_heartbeat**：定期 store 向 PD 汇报自己的相关信息，供 PD 做后续调度。譬如，Store 会告诉 PD 当前的磁盘大小，以及剩余空间，如果 PD 发现空间不够了，就不会考虑将其他的 Peer 迁移到这个 Store 上面。
>
> **ask_split/report_split**：当 Region 发现自己需要 split 的时候，就 ask_split 告诉 PD，PD 会生成新分裂 Region 的 ID ，当 Region 分裂成功之后，会 report_split 通知 PD。
>
> #### Peer 的创建方式
>
> 1. 主动创建，通常对于第一个 Region 的第一个副本 Peer，我们采用这样的创建方式，初始化的时候，我们会将它的 Log Term 和 Index 设置为 5。
> 2. 被动创建，当一个 Region 添加一个副本 Peer 的时候，当这个 ConfChange 命令被 applied 之后， Leader 会给这个新增 Peer 所在的 Store 发送 Message，Store 收到这个 Message 之后，发现并没有相应的 Peer 存在，并且确定这个 Message 是合法的，就会创建一个对应的 Peer，但此时这个 Peer 是一个未初始化的 Peer，不知道所在的 Region 任何的信息，我们使用 0 来初始化它的 Log Term 和 Index。Leader 就能知道这个 Follower 并没有数据（0 到 5 之间存在 Log 缺口），Leader 就会给这个 Follower 直接发送 snapshot。
> 3. Split 创建，当一个 Region 分裂成两个 Region，其中一个 Region 会继承分裂之前 Region 的元信息，只是会将自己的 Range 范围修改。而另一个 Region 相关的元信息，则会新建，新建的这个 Region 对应的 Peer，初始的 Log Term 和 Index 也是 5，因为这时候 Leader 和 Follower 都有最新的数据，不需要 snapshot。（注意：实际 Split 的情况非常的复杂，有可能也会出现发送 snapshot 的情况，但这里不做过多说明）。

### 代码阅读：

`kv/raftstore/runner/scheduler_task.go` ：

**region_heartbeat**：定期 Region 向 PD 汇报自己的相关信息，供 PD 做后续的调度。譬如，如果一个 Region 给 PD 上报的 peers 的数量小于预设的副本数，那么 PD 就会给这个 Region 添加一个新的副本 Peer。

收到回复后，根据回复进行 Peer 变更以及领导权 leader 的变更与转移，创建请求后通过 router 的 peerSender 最后交给 raftWorker 进行处理，代码如下：

![](C:\Users\new\Desktop\实习\project\image\sdgsdjhsdljfkls.png)

```go
r.router.SendRaftCommand(cmd, callback)

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(.......)
}

func (pr *router) send(regionID uint64, msg message.Msg) error {
	......
	pr.peerSender <- msg
	return nil
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

```

## Part C

调度器 Scheduler 拥有关于整个集群 cluster 的一些信息，如每一个 region 所在的位置，每个 region 的 startkey 以及 endkey 等等。

为了获取这些信息，Scheduler 要求每个 Region 定期向 Scheduler 发送心跳请求 `RegionHeartbeatRequest` ；当调度器 Scheduler 接收到这些信息后，会使用搜集到的区域信息， 检查 TinyKV 集群中是否存在不平衡问题，如存在 store 包含太多 region 等情况，如果需要进行 `ChangePeer` 或 `TransferLeader` Scheduler 会返回对应的响应 `RegionHeartbeatResponse` ，该响应会作为 `SchedulerTask` 进行处理，这些请求定义 `proto/pkg/schedulerpb/schedulerpb.pb.go` 中，`onRegionHeartbeatResponse` 处理流程：

```go
// peerMsgHandler 定期发送 heartbeat
func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}
// 发送 SchedulerRegionHeartbeatTask 到 schedulerWorker
func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	......
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}
// 调用 SchedulerClient 包中的 RegionHeartbeat 方法进行处理，
// 处理方法有两种，第一种是模拟的，第二种是实际运行的，
// 在后续代码实现可以参考模拟部分的实现方式
func (r *SchedulerTaskHandler) onHeartbeat(t *SchedulerRegionHeartbeatTask) {
	......
	req := &schedulerpb.RegionHeartbeatRequest{
		Region:          t.Region,
		Leader:          t.Peer,
		PendingPeers:    t.PendingPeers,
		ApproximateSize: uint64(size),
	}
	r.SchedulerClient.RegionHeartbeat(req)
}


func (r *SchedulerTaskHandler) onRegionHeartbeatResponse(resp *schedulerpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		......
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		......
	}
}
```

`proto/pkg/schedulerpb/schedulerpb.pb.go` ：

```go
type RegionHeartbeatRequest struct {
	Header *RequestHeader 
	Region *metapb.Region 
	Leader *metapb.Peer 
    // Pending peers are the peers that the leader can't consider as working followers.
	PendingPeers []*metapb.Peer  
	ApproximateSize      uint64   
}

type RegionHeartbeatResponse struct {
	Header 		*ResponseHeader 
	ChangePeer  *ChangePeer
	TransferLeader *TransferLeader 
	RegionId    uint64             
	RegionEpoch *metapb.RegionEpoch 
	TargetPeer  *metapb.Peer 
}
```

### The Code

要改的代码在 `scheduler/server/cluster.go` 及 `scheduler/server/schedulers/balance_region.go` 中。

如上所述，当 Scheduler 收到 region heartbeat 时，首先更新其本地 region 信息，然后检查这个区域是否有待处理（pending）的命令，如果有，它将作为 response 发回。

需要如下两个方法，首先是 **`processRegionHeartbeat`** 方法，在该方法中 Scheduler 会更新其本地状态；其次是 **`Schedule`** 方法，该方法起到平衡区域调度的功能，在该方法中 Scheduler 扫描 stores 并确认其平衡性，以决定是否需要将移动 region 。

### 搜集 region heartbeat

`processRegionHeartbeat` 函数的唯一参数是 regionInfo，包含有关此心跳发送方的 region 信息，Scheduler 只需要更新本地 region 信息记录即可，以下代码位于 `scheduler/server/core/region.go` 中

```go
type RegionInfo struct {
	meta            *metapb.Region
	learners        []*metapb.Peer
	voters          []*metapb.Peer
	leader          *metapb.Peer
	pendingPeers    []*metapb.Peer
	approximateSize int64
}

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *schedulerpb.RegionHeartbeatRequest) *RegionInfo {
    ......
	region := &RegionInfo{
		meta:            heartbeat.GetRegion(),
		leader:          heartbeat.GetLeader(),
		pendingPeers:    heartbeat.GetPendingPeers(),
		approximateSize: int64(regionSize),
	}
	classifyVoterAndLearner(region)
	return region
}
```

需要注意的是，Scheduler **不能信任每一个 heartbeat**，尤其是由于网络隔离会使得有些部分产生 partitions（集群分区隔离问题），这些结点的信息可能存在错误。例如，当某些 region 进行重新选举以及 region split 后，leader 以及 confversion、version 发生改变，而另一部分孤立的结点仍保持原有的状态，导致一个 region 同时存在两个leader 且 version 不相同的情况。

当遇到这种由于网路隔离导致的 partition 问题时，Scheduler 应该使用  `conf_ver` 以及 `version` 进行判别与确认。**首先，调度器 Scheduler 应该比较两个节点的 Region version 值，如果 version 值相同，则 Scheduler 比较 conf_ver 值，具有较大 conf_ver 的节点拥有更新的信息**。

具体的检查流程如下：

1. 检查本地 storage 中是否存在一个 region 其 id 与`processRegionHeartbeat` 中的 id 相同；然后如果 heartbeat 的 `conf_ver` 或 `version` 小于本地状态表明该 heartbeat 的信息的陈旧的，不应该采纳。
2. 如果本地存储中不存在这样的 region，则需要扫描所有与 heartbeat 中的 region 存在重叠（overlap）的 region，只有**当 heartbeat 的 `conf_ver` 与 `version` 大于等于所有重叠 region 的 `conf_ver` 与 `version`** 才可以接受当前 heartbeat。

当确认某个 heartbeat 不是陈旧的，那么就可以接受当前 heartbeat，在接受后是否需要更新本地 region 信息可以使用以下一系列的条件进行判断：

1. 如果新的 `version` 或 `conf_ver`大于原本的值，则不能跳过更新
2. 如果leader变了，就不能跳过更新
3. 如果新的或原本的 region 信息中包含 pending peer，则不能跳过更新
4. 如果 ApproximateSize 改变，则不能跳过更新

如果 Scheduler 确定使用 heartbeat 的 region 信息更新本地状态，则需要更新两个部分：region tree 以及 store status，其中 region tree 包含在 `RaftCluster.core.PutRegion` 中，相关的 store status 包含在 `RaftCluster.core.UpdateStoreStatus` 中（如领导者数 leader count、区域数 region count、挂起的 peer 数 pending peer count...）

### 代码实现：

参考 `kv/test_raftstore/scheduler.go` 中 `RegionHeartbeat` 的实现方式，至少需要更新如`version` 、 `conf_ver`、leader 以及 pending 等信息：

```go
func (m *MockSchedulerClient) RegionHeartbeat(req *schedulerpb.RegionHeartbeatRequest) error {
	.......
	m.pendingPeers[p.GetId()] 
	m.leaders[regionID] = req.Leader
	m.handleHeartbeatVersion(req.Region)
	m.handleHeartbeatConfVersion(req.Region)
    ......
}
```

1. 使用 `c.GetRegion(region.GetID())` 的方式查找本地是否含有与 heartbeat 中相同的 region

2. 如果 region 不存在，则需要扫描所有与 heartbeat 中的 region 存在重叠（overlap）的 region，使用方法`c.ScanRegions` 扫描本地的 region，注意当 limit <= 0 表示 no limit；然后，使用 `util.IsEpochStale`方法判断 region 是否是陈旧的。如果与所有重叠的 region 比较后，region 仍然不是陈旧的，转到4

3. 如果 region 存在，则使用 `util.IsEpochStale`方法判断 region 是否是陈旧的，region 不是陈旧的，转到4

4. **使用 `c.putRegion(region)` 方法更新 region 信息，使用 `c.updateStoreStatusLocked()` 方法更新 store status 信息**。由于当确认此 heartbeat 不是陈旧的，那么可以根据此信息进行更新，为加快更新速度可以增加一些判断条件加快更新，如新的 `version` 或 `conf_ver`大于原值，则需更新，leader变了，则需更新，新的或原本的 region 信息中包含 pending peer，则需更新， ApproximateSize 改变，则需更新等等。

   **PS. 但是此处存在问题，就是所给的条件只能判断如果出现这些是一定要更新的，如果找不到所有需要更新的条件，这个消息就不能被跳过，所以本次选择不判断，直接更新，冗余的更新不会影响最终的结果。**

5. 更新的流程如下：

   （1）删除原来的 region 信息，如果存在重叠，则删除所有重叠的 region 

   （2）将新的 region 信息加入 region tree 与 region map 中，同时更新 leaders、followers、learners 以及 pendingPeers

   以上两步的更新可以使用  `c.putRegion(region)` 方法更新，该方法已将以上步骤封装完成，代码如下：

   （3）更新相关的 store status（如领导者数 leader count、区域数 region count、挂起的 peer 数 pending peer count 等），该部分使用方法 `c.updateStoreStatusLocked(storeId)` 进行更新即可。

   ```go
   func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
   	c.core.PutRegion(region)
   	return nil
   }
   
   func (bc *BasicCluster) PutRegion(region *RegionInfo) []*RegionInfo {
   	return bc.Regions.SetRegion(region)
   }
   
   func (r *RegionsInfo) SetRegion(region *RegionInfo) []*RegionInfo {
   	if origin := r.regions.Get(region.GetID()); origin != nil {
   		r.RemoveRegion(origin)
   	}
   	return r.AddRegion(region)
   }
   
   func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
   	// Remove from tree and regions.
   	r.tree.remove(region)
   	r.regions.Delete(region.GetID())
   	// Remove from leaders and followers.
   	for _, peer := range region.meta.GetPeers() {
   		storeID := peer.GetStoreId()
   		r.leaders[storeID].remove(region)
   		r.followers[storeID].remove(region)
   		r.learners[storeID].remove(region)
   		r.pendingPeers[storeID].remove(region)
   	}
   }
   
   func (r *RegionsInfo) AddRegion(region *RegionInfo) []*RegionInfo {
   	// Add to tree and regions & delete overlap regions
   	overlaps := r.tree.update(region)
   	for _, item := range overlaps {
   		r.RemoveRegion(r.GetRegion(item.GetID()))
   	}
   
   	r.regions.Put(region)
   	// Add to leaders and followers.
   	for _, peer := range region.GetVoters() {
   		storeID := peer.GetStoreId()
   		if peer.GetId() == region.leader.GetId() {
   			// Add leader peer to leaders.
   			store, ok := r.leaders[storeID]
   			if !ok {
   				store = newRegionSubTree()
   				r.leaders[storeID] = store
   			}
   			store.update(region)
   		} else {
   			// Add follower peer to followers.
   			store, ok := r.followers[storeID]
   			if !ok {
   				store = newRegionSubTree()
   				r.followers[storeID] = store
   			}
   			store.update(region)
   		}
   	}
       // Add to learners.
   	for _, peer := range region.GetLearners() {
   		storeID := peer.GetStoreId()
   		store, ok := r.learners[storeID]
   		if !ok {
   			store = newRegionSubTree()
   			r.learners[storeID] = store
   		}
   		store.update(region)
   	}
       // Add to pendingPeers.
   	for _, peer := range region.pendingPeers {
   		storeID := peer.GetStoreId()
   		store, ok := r.pendingPeers[storeID]
   		if !ok {
   			store = newRegionSubTree()
   			r.pendingPeers[storeID] = store
   		}
   		store.update(region)
   	}
   	return overlaps
   }
   
   
   ```

   为了更好地理解系统，查看 region 在 PD 调度器中的保存形式，如下所示，主要使用 `regionTree` 以及 `regionMap` 两个结构存储数据。 

   其中 tree 中保存着全局的 region 信息，主要用于**使用 Key 进行查找**以及查**询是否存在重叠的 region 时**使用；而 regions 为 regionMap，可以很方便地使用 regionID 查找对应的 regionInfo。

   leaders、followers、learners 以及 pendingPeers 为 `regionSubTree` ，该类为  `regionTree` 的封装，这4个量的作用是保存每个 store 中各个 region 的信息，**主要作用是用于后期 region 调度时进行选择**。

   ```go
   type RegionsInfo struct {
   	tree         *regionTree
   	regions      *regionMap                // regionID -> regionInfo
   	leaders      map[uint64]*regionSubTree // storeID -> regionSubTree
   	followers    map[uint64]*regionSubTree // storeID -> regionSubTree
   	learners     map[uint64]*regionSubTree // storeID -> regionSubTree
   	pendingPeers map[uint64]*regionSubTree // storeID -> regionSubTree
   }
   ```

   

### 实现 region 平衡 scheduler

Scheduler 中可以运行多种不同类型的调度器，例如 balance-region 调度器和 balance-leader 调度器，本学习材料将重点介绍 balance-region 调度器的实现。

每个调度器都应该实现了调度器接口 interface（定义在`/scheduler/server/schedule/scheduler.go` 中），调度器将使用 `GetMinInterval` 的返回值作为默认间隔来 定期运行 `Schedule` 方法。

```go
type Scheduler interface {
	http.Handler
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	EncodeConfig() ([]byte, error)
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster opt.Cluster) error
	Cleanup(cluster opt.Cluster)
	Schedule(cluster opt.Cluster) *operator.Operator
	IsScheduleAllowed(cluster opt.Cluster) bool
}
```

如果进过多次调用后 `Schedule` 方法仍然返回 null，则需要使用`GetNextInterval`方法增加间隔，通过定义`GetNextInterval`可以改变间隔 Interval 增加的方式；如果返回  `Operator` ，则 Scheduler 将调度 `Operator` 作为相关区域的下一次心跳的响应。

Scheduler 接口的核心部分是`Schedule`方法，该方法的返回值为`Operator`，其中包含多个区域调整动作步骤，包括`AddPeer `以及`RemovePeer`。

如 `MovePeer` 可能包含 `AddPeer`, `transferLeader` 以及 `RemovePeer` 。以下图为例， scheduler 尝试将 peers 从3号 store 转移到 4号 store，转移流程入下：

1. First, it should `AddPeer` for the fourth store.  **PS. 新增的 peer id 应该为新分配的id**

2. Then it checks whether the third is a leader, and find that no, it isn’t, so there is no need to `transferLeader`. 

3. Then it removes the peer in the third store.


您可以使用包中的`CreateMovePeerOperator`函数`scheduler/server/schedule/operator`来创建 `MovePeer` operator。

[![balance](https://github.com/tidb-incubator/tinykv/raw/course/doc/imgs/balance1.png)](https://github.com/tidb-incubator/tinykv/blob/course/doc/imgs/balance1.png)

[![balance](https://github.com/tidb-incubator/tinykv/raw/course/doc/imgs/balance2.png)](https://github.com/tidb-incubator/tinykv/blob/course/doc/imgs/balance2.png)

在这一部分，只需要实现`scheduler/server/schedulers/balance_region.go`中的`Schedule`方法，这个 scheduler 避免了一个 store 中有太多的 region。简易的调度流程入下：首先，调度器将**选择所有合适的 store**；然后根据它们的区域大小对它们进行排序；**然后**，调度器尝试从具有**最大区域大小**的 store 中找到要移动的区域。

调度器将尝试找到最适合在商店中移动的区域。首先，它会尝试选择一个挂起的区域，因为挂起可能意味着磁盘过载。如果没有挂起的区域，它将尝试找到一个跟随区域。如果它仍然无法选择一个区域，它将尝试选择领导区域。最后，它会选择要移动的区域，或者调度器将尝试下一个具有较小区域大小的商店，直到所有商店都被尝试过（The scheduler will try to find the region most suitable for moving in the store. First, it will try to **select a pending region** because pending may mean the disk is overloaded. If there **isn’t a pending region, it will try to find a follower region**. If it **still cannot pick out one region, it will try to pick leader regions**. Finally, it will select out the region to move, or the Scheduler will try the next store which has a smaller region size until all stores will have been tried.）

选择一个区域进行移动后，调度器将选择一个商店作为目标。实际上，调度器会选择区域大小最小的商店。然后调度器会通过检查原始商店和目标商店的区域大小之间的差异来判断此移动是否有价值。如果差异足够大，Scheduler 应该在目标存储上分配一个新的 peer 并创建一个 move peer 操作符（After you pick up one region to move, the Scheduler will select a store as the target. Actually, **the Scheduler will select the store with the smallest region size**. Then the Scheduler will judge whether this movement is valuable, by checking the difference between region sizes of the original store and the target store. If the difference is big enough, the Scheduler should allocate a new peer on the target store and create a move peer operator.）

您可能已经注意到，上面的例程只是一个粗略的过程，留下了很多问题：

- 哪些 store 适合移动？

简而言之，一个合适的 store 应该是运行的，down 时间不能比 cluster  集群的 `MaxStoreDownTime ` 时间长，该值可以通过 `cluster.GetMaxStoreDownTime()` 获得。

- 如何选择 region？

Scheduler 框架提供了三种获取区域的方法。`GetPendingRegionsWithLock`，`GetFollowersWithLock`和`GetLeadersWithLock`，调度器可以从中获取相关区域，然后你可以选择一个随机区域。

- 如何判断这个操作是否有价值？

如果原始和目标商店的区域大小差异太小，我们将区域从原始商店移动到目标商店后，调度器可能想下次再次移动回来。**所以我们要保证这个差值要大于大约区域大小的两倍**，这样才能保证移动后目标商店的区域大小仍然小于原来的商店。

### 代码实现：

可以参考 `scheduler/server/schedulers/balance_leader.go` 中 balance-leader 调度器的实现方式，尤其是移动 peer 的操作。

1. 调度器将**选择所有合适的 store**（一个合适的 store 应该是运行的，down 时间不能比 cluster  集群的 `MaxStoreDownTime ` 时间长，该值可以通过 `cluster.GetMaxStoreDownTime()` 获得）；然后根据它们的区域大小对它们进行排序

2. 调度器尝试从具有**最大区域大小**的 store 中找到要移动的区域

3. The scheduler will try to find the region most suitable for moving in the store. First, it will try to **select a pending region** because pending may mean the disk is overloaded. If there **isn’t a pending region, it will try to find a follower region**. If it **still cannot pick out one region, it will try to pick leader regions**. Finally, it will select out the region to move, or the Scheduler will try the next store which has a smaller region size until all stores will have been tried.

   使用如下方式，调用方法 `GetPendingRegionsWithLock`：

   ```go
   var regionInfo *core.RegionInfo
   cluster.GetPendingRegionsWithLock(stores[i].GetID(), 
   	func(regions core.RegionsContainer){regionInfo = regions.RandomRegion(nil,nil)})
   ```

4. After you pick up one region to move, the Scheduler will select a store as the target. Actually, **the Scheduler will select the store with the smallest region size**. Then the Scheduler will judge whether this movement is valuable, by checking the difference between region sizes of the original store and the target store. 

   **需要注意的是，由于同一个 store 不能存在两个相同的 region，因此选择 target 时要将具有此 region 的 store 剔除，否则将产生错误。**

   ```go
   difference := source.GetRegionSize() - target.GetRegionSize()
   if difference <= 2 * regionInfo.GetApproximateSize() {
   	return nil
   }
   ```

5. If the difference is big enough, the Scheduler should allocate a new peer on the target store and create a move peer operator.

   ```go
   // allocate a new peer on the target store
   newPeer, err := cluster.AllocPeer(target.GetID())
   ......
   // create a move peer operator
   peerOperator, err := operator.CreateMovePeerOperator("balance-region", cluster,
   	regionInfo, operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())
   ......
   ```

6. **为了通过测试，需要将 region 数与 MaxReplicas 进行比较，如果小于 MaxReplicas 表示当前 region 为完成初始化，该条指南中未写出，特此说明。**

   ```go
   storeIds := regionInfo.GetStoreIds()
   if len(storeIds) < cluster.GetMaxReplicas() {
   	return nil
   }
   ```

   

   



















