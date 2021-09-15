# Project 4: Transactions

事务系统 transaction system 是客户端（TinySQL）和服务器（TinyKV）之间的协作协议，双方必须正确实施才能确保交易属性。我们将为事务请求提供完整的 API，独立于您在 project1 中实现的原始请求（实际上，如果客户端同时使用原始 API 和事务 API，我们无法保证事务属性）。

事务承诺  [*snapshot isolation* (SI)](https://en.wikipedia.org/wiki/Snapshot_isolation) ，这意味着在每个事务 transaction 中，客户端将从数据库中读取，就好像它在事务开始时被及时冻结（事务看到数据库的*一致*视图）。一个事务要么全部写入数据库，要么都不写入（如果它与另一个事务冲突）（Transactions promise [*snapshot isolation* (SI)](https://en.wikipedia.org/wiki/Snapshot_isolation). That means that within a transaction, a client will read from the database as if it were frozen in time at the start of the transaction (the transaction sees a *consistent* view of the database). Either all of a transaction is written to the database or none of it is if it conflicts with another transaction）

要提供 SI，您需要更改数据在后备存储中的存储方式。您将存储一个键的值和一个时间（由时间戳表示），而不是为每个键存储一个值。这称为多版本并发控制 (MVCC)，因为每个键都存储了一个值的多个不同版本（To provide SI, you need to change the way that data is stored in the backing storage. **Rather than storing a value for each key, you'll store a value for a key and a time** (represented by a timestamp). This is called multi-version concurrency control (MVCC) because multiple different versions of a value are stored for every key.）

You'll implement MVCC in part A. In parts B and C you’ll implement the transactional API.

## Transactions in TinyKV

TinyKV 的事务设计遵循 [Percolator](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf) ；它是一个两阶段提交协议（two-phase commit protocol，2PC）。

事务是一系列读取和写入的组合（list of reads and writes），一个事务有一个开始时间戳（start timestamp），当一个事务被提交后，将拥有一个提交时间戳（commit timestamp 必须大于 开始时间戳）。

整个交易 transaction 从在开始时间戳有效的密钥版本中读取（reads from the version of a key that is valid at the start timestamp），也即是读取的时间戳小于等于 start timestamp。提交后，所有写入请求在提交时间戳处写入，并标记为提交时间戳（all writes appear to have been written at the commit timestamp）。在开始时间戳和提交时间戳之间的任何其他事务都不能写入任何要写入的密钥，否则整个事务将被取消（这称为写入冲突）。Any key to be written must not be written by any other transaction between the start and commit timestamps, otherwise, the whole transaction is canceled (this is called a write conflict).

该协议从客户端从 TinyScheduler 获取开始时间戳开始。然后，它在本地建立事务 transaction 处理，从数据库（使用`KvGet`或`KvScan`请求进行读取，注意这两种方法包含开始时间戳），而只是在存储器本地记录写入。

事务建立后，客户端会选择一个键作为*主键*（注意这与SQL主键无关）。客户端向TinyKV 发送 `KvPrewrite` 消息，`KvPrewrite`消息包含事务中的所有写入。TinyKV 服务器将尝试锁定事务所需的所有密钥（attempt to lock all keys required by the transaction）。如果**存在锁定密钥失败**，则 TinyKV **会向客户端响应事务失败**。客户端可以稍后重试事务（即，使用不同的开始时间戳）。**如果所有密钥都被锁定，则预写成功，每个锁 lock 保存事务的主键和生存时间（TTL）**。

实际上，由于事务中的密钥 keys 可能在多个区域 region 中，存储在不同的 Raft group 中，**因此客户端会发送多个`KvPrewrite`请求，每个区域领导者 leader 一个 prewrite 请求，每个 prewrite 仅包含对该区域的修改**。如果所有预写成功，则客户端将向所有包含主键的 region 发送提交请求（commit request）。提交请求将包含一个提交时间戳（客户端也从 TinyScheduler 获得），这是提交事务写入并因此对其他事务可见的时间。

如果存在任意一个预写失败（any prewrite fails），则客户端通过向所有区域发送请求 `KvBatchRollback` 来回滚事务（解锁事务中的所有密钥 unlock all keys，并删除预写值）。

在 TinyKV 中，**TTL 检查不是自发执行的**。为了启动超时检查，**客户端使用`KvCheckTxnStatus`请求，将当前时间 current time 发送到 TinyKV** 。该请求通过其主键和开始时间戳标识事务。锁可能丢失或已经提交；如果不是，TinyKV 会将锁上的 TTL 与`KvCheckTxnStatus`**请求中的时间戳**进行比较（The lock may be missing or already be committed; if not, TinyKV compares the TTL on the lock with the timestamp in the `KvCheckTxnStatus` request.）。

如果锁定超时，则 TinyKV 回滚锁定。在任何情况下，TinyKV 都会响应锁的状态，以便客户端可以通过发送`KvResolveLock`请求来采取行动。当客户端由于另一个事务的锁而无法预写一个事务时，通常会检查事务状态。If the lock has timed out, then TinyKV rolls back the lock. In any case, TinyKV responds with the status of the lock so that the client can take action by sending a `KvResolveLock` request. **The client typically checks transaction status** when it fails to prewrite a transaction due to another transaction's lock.

如果主键提交成功，则客户端将提交其他区域中的所有其他键。这些请求应该总是成功的，因为通过积极响应预写请求，服务器承诺如果它收到该事务的提交请求，那么它就会成功。一旦客户端获得所有预写响应，事务失败的唯一方法就是超时，在这种情况下，提交主键应该失败。一旦提交了主键，其他键就不能再超时了。

If the primary key commit succeeds, then the client will commit all other keys in the other regions. These requests should always succeed because by responding positively to a prewrite request, the server is promising that if it gets a commit request for that transaction then it will succeed. Once the client has all its prewrite responses, the only way for the transaction to fail is if it times out, and in that case, committing the primary key should fail. Once the primary key is committed, then the other keys can no longer time out. 

如果主键提交失败，那么客户端将通过发送`KvBatchRollback`请求来回滚事务。

If the primary commit fails, then the client will rollback the transaction by sending `KvBatchRollback` requests.

## Part A

您在早期项目中实现的原始 API 将用户键和值直接映射到存储在底层存储中的键和值 （The raw API you implemented in earlier projects **maps user keys and values directly to keys and values stored in the underlying storage Badger**）。由于 Badger 不知道分布式事务层（distributed transaction layer），因此您必须在 TinyKV 中处理事务，并将用户键和值**编码（encode）到底层存储**中。这是使用多版本并发控制 (MVCC) 实现的。在本项目中，您将在 TinyKV 中实现 MVCC 层。

实现 MVCC 意味着使用简单的键/值 API 来表示事务 API（representing the transactional API using a simple key/value API），TinyKV 不是为每个 key 存储一个 value，而是为存储所有版本的 values（every version of a value for each key）。

TinyKV 使用三个列族 (CF)：`default`保存用户值、`lock`存储锁和`write`记录更改。该`lock`CF是使用用户密钥访问; 它存储一个序列化的`Lock`数据结构（在[lock.go 中](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/lock.go)定义）。该`default`CF是使用用户密钥和所访问的*开始*在其被写入的事务的时间戳; 它仅存储用户值。该`write`CF是使用用户密钥访问，并且*提交*在其被写入的事务的时间戳; 它存储一个`Write`数据结构（在[write.go 中](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/write.go)定义）。TinyKV uses three column families (CFs): `default` to hold user values, `lock` to store locks, and `write` to record changes. The `lock` CF is accessed using the user key; it stores a serialized `Lock` data structure (defined in [lock.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/lock.go)). The `default` CF is accessed using the user key and the *start* timestamp of the transaction in which it was written; it stores the user value only. The `write` CF is accessed using the user key and the *commit* timestamp of the transaction in which it was written; it stores a `Write` data structure (defined in [write.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/write.go)).

用户密钥和时间戳被组合成一个*编码密钥*。密钥的编码方式是，编码密钥的升序首先按用户密钥（升序）排序，然后按时间戳（降序）排序。这确保迭代编码的密钥将首先给出最新的版本。用于编码和解码密钥的辅助函数在[transaction.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction.go)中定义。**A user key and timestamp are combined into an *encoded key***. Keys are encoded in such a way that the ascending order of encoded keys orders first by user key (ascending), then by timestamp (descending). This ensures that iterating over encoded keys will give the most recent version first. Helper functions for encoding and decoding keys are defined in [transaction.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction.go).

需要实现一个名为  `MvccTxn` 的简单结构，在 B 和 C 部分，将使用`MvccTxn`API 来实现事务 API。`MvccTxn`提供基于用户密钥和锁、写和值的逻辑表示的读和写操作（**`MvccTxn` provides read and write operations based on the user key and logical representations of locks, writes, and values**）。修改被收集在 `MvccTxn` 中，`MvccTxn`一旦收集到一个命令的所有修改，它们将被一次性写入底层数据库。这可确保命令修改成功或失败的原子性（atomically）。请注意，MVCC 事务与 TinySQL 事务不同，MVCC 事务包含对单个命令的修改，而不是一系列命令（**An MVCC transaction contains the modifications for a single command, not a sequence of commands**）。

`MvccTxn`在[transaction.go 中](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction.go)定义。有一个存根实现，以及一些用于编码和解码密钥的辅助函数。测试在[transaction_test.go 中](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction_test.go)。对于本练习，您应该实现每个`MvccTxn`方法，以便所有测试都通过。每个方法都记录了其预期行为。`MvccTxn` is defined in [transaction.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction.go). There is a stub implementation, and some helper functions for encoding and decoding keys. Tests are in [transaction_test.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction_test.go). For this exercise, you should implement each of the `MvccTxn` methods so that all tests pass. Each method is documented with its intended behavior.

> Hints:
>
> - An `MvccTxn` should know the **start timestamp of the request** it is representing.
> - 实现最具挑战性的方法可能是`GetValue`检索写入的方法。您将需要使用`StorageReader`来迭代 CF。请记住编码密钥的顺序，并记住决定值何时有效取决于提交时间戳，而不是事务的开始时间戳。The most challenging methods to implement are likely to **be `GetValue` and the methods for retrieving writes**. You will need to **use `StorageReader` to iterate over a CF**. Bear in mind the ordering of encoded keys, and remember that when deciding when a value is valid depends on the commit timestamp, not the start timestamp, of a transaction.

## Part B

在这一部分中，您将使用 `MvccTxn` 实现操作 `KvGet`，`KvPrewrite`和`KvCommit`请求的处理流程。如上所述，`KvGet`在提供的时间戳从数据库读取值（`KvGet` reads a value from the database **at a supplied timestamp**）。如果要读取的密钥在`KvGet`请求时被另一个事务锁定，则 TinyKV 应返回错误 error；否则，TinyKV 必须搜索密钥的版本以找到最新的有效值（TinyKV must search the versions of the key to find the most recent, valid value）。

`KvPrewrite`并分`KvCommit`两个阶段将值写入数据库。两个请求都对多个键进行操作，但实现可以独立处理每个键 `KvPrewrite` and `KvCommit` write values to the database in two stages. Both requests operate on multiple keys, but the implementation can handle each key independently.

`KvPrewrite`是实际将值写入数据库的位置。一个键被锁定并存储一个值。我们必须检查另一个交易是否没有锁定或写入相同的密钥。`KvPrewrite` is where a value is actually written to the database. A key is locked and a value stored. We must check that another transaction has not locked or written to the same key.

`KvCommit`不会更改数据库中的值，但会记录该值已提交。`KvCommit`如果密钥未锁定或被另一个事务锁定，则将失败。`KvCommit` does not change the value in the database, but it does record that the value is committed. `KvCommit` will fail if the key is not locked or is locked by another transaction.

你将需要实现`KvGet`，`KvPrewrite`以及`KvCommit`在定义的方法[server.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/server/server.go)。每个方法都接受一个请求对象并返回一个响应对象，您可以通过查看[kvrpcpb.proto](https://github.com/tidb-incubator/tinykv/blob/course/proto/kvrpcpb.proto)中的协议定义来查看这些对象的内容（您应该不需要更改协议定义）。You'll need to implement the `KvGet`, `KvPrewrite`, and `KvCommit` methods defined in [server.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/server/server.go). Each method **takes a request object and returns a response object**, you can see the contents of these objects by looking at the protocol definitions in [kvrpcpb.proto](https://github.com/tidb-incubator/tinykv/blob/course/proto/kvrpcpb.proto) (you shouldn't need to change the protocol definitions).

TinyKV 可以同时处理多个请求，因此存在本地竞争条件的可能性。例如，TinyKV 可能会同时收到来自不同客户端的两个请求，其中一个提交一个密钥，另一个回滚相同的密钥。为避免竞争条件，您可以*锁存*数据库中的任何键。这个锁存器的工作方式很像每个键的互斥锁。一个闩锁覆盖所有 CF。[Latches.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/latches/latches.go)定义了一个`Latches`为此提供 API的对象。TinyKV can handle multiple requests concurrently, so there is the possibility of local race conditions. For example, TinyKV might receive two requests from different clients at the same time, one of which commits a key, and the other rolls back the same key. **To avoid race conditions, you can *latch* any key in the database**. This latch works much like a per-key mutex. One latch covers all CFs. [latches.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/latches/latches.go) defines a `Latches` object which provides API for this.

> Hints:
>
> - All commands will be a part of a transaction. Transactions are identified by a start timestamp (aka start version).
> - 任何请求都可能导致区域错误，这些应该以与原始请求相同的方式处理。对于诸如钥匙被锁定之类的情况，大多数响应都有一种指示非致命错误的方法。通过将这些报告给客户端，它可以在一段时间后重试事务。**Any request might cause a region error, these should be handled in the same way as for the raw requests. Most responses have a way to indicate non-fatal errors for situations like a key being locked. By reporting these to the client, it can retry a transaction after some time.**

## Part C

在这一部分中，您将实现`KvScan`，`KvCheckTxnStatus`，`KvBatchRollback`，和`KvResolveLock`。在高层次上，这类似于 B 部分 - 在[server.go 中](https://github.com/tidb-incubator/tinykv/blob/course/kv/server/server.go)使用`MvccTxn`。In this part, you will implement `KvScan`, `KvCheckTxnStatus`, `KvBatchRollback`, and `KvResolveLock`. At a high-level, this is similar to part B - implement the gRPC request handlers in [server.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/server/server.go) using `MvccTxn`.

`KvScan`是 的事务等价物`RawScan`，它从数据库中读取许多值。但是就像`KvGet`，它在一个时间点这样做。由于 MVCC，`KvScan`比`RawScan`- 由于多个版本和密钥编码，您不能依赖底层存储来迭代值。

`KvCheckTxnStatus`, `KvBatchRollback`, 和`KvResolveLock`由客户端在尝试编写事务时遇到某种冲突时使用。每个都涉及更改现有锁的状态。

`KvCheckTxnStatus` 检查超时，删除过期的锁并返回锁的状态。

`KvBatchRollback` 检查密钥是否被当前事务锁定，如果是，则删除锁定，删除任何值并留下回滚指示符作为写入。

`KvResolveLock` 检查一批锁定的密钥并将它们全部回滚或全部提交。`KvScan` is the transactional equivalent of `RawScan`, it reads many values from the database. But like `KvGet`, it does so at a single point in time. Because of MVCC, `KvScan` is significantly more complex than `RawScan` - you can't rely on the underlying storage to iterate over values because of multiple versions and key encoding.

`KvCheckTxnStatus`, `KvBatchRollback`, and `KvResolveLock` are used by a client when it encounters some kind of conflict when trying to write a transaction. Each one involves changing the state of existing locks.

`KvCheckTxnStatus` checks for timeouts, removes expired locks and returns the status of the lock.

`KvBatchRollback` checks that a key is locked by the current transaction, and if so removes the lock, deletes any value and leaves a rollback indicator as a write.

`KvResolveLock` inspects a batch of locked keys and either rolls them all back or commits them all.

> Hints:
>
> - For scanning, you might find it helpful to implement your own scanner (iterator) abstraction which iterates over logical values, rather than the raw values in underlying storage. `kv/transaction/mvcc/scanner.go` is a framework for you.
> - When scanning, some errors can be recorded for an individual key and should not cause the whole scan to stop. For other commands, any single key causing an error should cause the whole operation to stop.
> - Since `KvResolveLock` either commits or rolls back its keys, you should be able to share code with the `KvBatchRollback` and `KvCommit` implementations.
> - A timestamp consists of a physical and a logical component. The physical part is roughly a monotonic version of wall-clock time. Usually, we use the whole timestamp, for example when comparing timestamps for equality. However, when calculating timeouts, we must only use the physical part of the timestamp. To do this you may find the `PhysicalTime` function in [transaction.go](https://github.com/tidb-incubator/tinykv/blob/course/kv/transaction/mvcc/transaction.go) useful.



The transaction package implements TinyKV's 'transaction' layer. This takes incoming requests from the kv/server/server.go as input and turns them into reads and writes of the underlying key/value store (defined by Storage in kv/storage/storage.go). **The storage engine handles communicating with other nodes and writing data to disk**. The transaction layer must translate high-level TinyKV commands into low-level raw key/value commands and ensure that processing of commands do not interfere with processing other commands.

Note that there are **two kinds of transactions** in play: **TinySQL transactions are collaborative between TinyKV and its client** (e.g., TinySQL). They are implemented using multiple TinyKV requests and ensure that multiple SQL commands can be executed atomically. There are also **mvcc transactions which are an implementation detail of this layer in TinyKV** (represented by MvccTxn in transaction.go). These ensure that **a *single* request is executed atomically**.

***Locks* are used to implement TinySQL transactions**. Setting or checking a lock in a TinySQL transaction is lowered to writing to the underlying store. ***Latches* are used to implement mvcc transactions** and are not visible to the client. They are stored outside the underlying storage (or equivalently, you can think of every key having its own latch). See the latches package for details.

**Within the `mvcc` package, `Lock` and `Write` provide abstractions for lowering locks and writes into simple keys and values**. The mvcc strategy is essentially to store all data (committed and uncommitted) at every point in time. So for example, if we store a value for a key, then store another value (a logical overwrite) at a later time, both values are preserved in the underlying storage.

This is implemented by **encoding user keys with their timestamps** (the starting timestamp of the transaction in which they are written) to make an encoded key (see codec.go). **The `default` CF is a mapping from encoded keys to their values**. **Locking a key means writing into the `lock` CF**. In this CF, we use the user key (i.e., not the encoded key so that a key is locked for all timestamps). The value in the `lock` CF consists of the 'primary key' for the transaction, the kind of lock (for 'put', 'delete', or 'rollback'), the start timestamp of the transaction, and the lock's ttl (time to live). See lock.go for the implementation.

#### **The status of values is stored in the `write` CF**. Here we map **keys encoded with their commit timestamps** (i.e., the time at which a transaction is committed) to a value containing the transaction's starting timestamp, and the kind of write ('put', 'delete', or 'rollback'). Note that for transactions which are rolled back, the start timestamp is used for the commit timestamp in the encoded key.

# 代码实现 & 相关知识

## 相关知识

在 **多版本并发控制（Multi-Version Concurrency Control，以下简称 MVCC）**中，每当想要更改或者删除某个数据对象时，DBMS 不会在原地去删除或这修改这个已有的数据对象本身，而是创建一个该数据对象的新的版本，这样的话同时并发的读取操作仍旧可以读取老版本的数据，而写操作就可以同时进行。这个模式的好处在于，可以让读取操作不再阻塞，事实上根本就不需要锁。

#### 读写事务

1. 事务提交前，在客户端 buffer 所有的 update/delete 操作。
2. Prewrite 阶段:

首先在所有行的写操作中选出一个作为 primary，其他的为 secondaries。

PrewritePrimary: 对 primaryRow 写入 L 列(上锁)，L 列中记录本次事务的开始时间戳。写入 L 列前会检查:

1. 是否已经有别的客户端已经上锁 (Locking)。
2. 是否在本次事务开始时间之后，检查 W 列，是否有更新 [startTs, +Inf) 的写操作已经提交 (Conflict)。

在这两种种情况下会返回事务冲突。否则，就成功上锁。将行的内容写入 row 中，时间戳设置为 startTs。

将 primaryRow 的锁上好了以后，进行 secondaries 的 prewrite 流程:

1. 类似 primaryRow 的上锁流程，只不过锁的内容为事务开始时间及 primaryRow 的 Lock 的信息。
2. 检查的事项同 primaryRow 的一致。

当锁成功写入后，写入 row，时间戳设置为 startTs。

1. 以上 Prewrite 流程任何一步发生错误，都会进行回滚：删除 Lock，删除版本为 startTs 的数据。
2. 当 Prewrite 完成以后，进入 Commit 阶段，当前时间戳为 commitTs，且 commitTs> startTs :

1. commit primary：写入 W 列新数据，时间戳为 commitTs，内容为 startTs，表明数据的最新版本是 startTs 对应的数据。
2. 删除L列。

如果 primary row 提交失败的话，全事务回滚，回滚逻辑同 prewrite。如果 commit primary 成功，则可以异步的 commit secondaries, 流程和 commit primary 一致， 失败了也无所谓。

#### 事务中的读操作

1. 检查该行是否有 L 列，时间戳为 [0, startTs]，如果有，表示目前有其他事务正占用此行，如果这个锁已经超时则尝试清除，否则等待超时或者其他事务主动解锁。注意此时不能直接返回老版本的数据，否则会发生幻读的问题。
2. 读取至 startTs 时该行最新的数据，方法是：读取 W 列，时间戳为 [0, startTs], 获取这一列的值，转化成时间戳 t, 然后读取此列于 t 版本的数据内容。

由于锁是分两级的，primary 和 seconary，只要 primary 的行锁去掉，就表示该事务已经成功 提交，这样的好处是 secondary 的 commit 是可以异步进行的，只是在异步提交进行的过程中 ，如果此时有读请求，可能会需要做一下锁的清理工作。

`TiKV`将 Key-Value，Locks 和 Writes 分别储存在`CF_DEFAULT`，`CF_LOCK`，`CF_WRITE`中，以如下格式进行编码

|           | default                        | lock                                  | write                           |
| :-------- | :----------------------------- | :------------------------------------ | :------------------------------ |
| **key**   | z{encoded_key}{start_ts(desc)} | z{encoded_key}                        | z{encoded_key}{commit_ts(desc)} |
| **value** | {value}                        | {flag}{primary_key}{start_ts(varint)} | {flag}{start_ts(varint)}        |

Details can be found [here ](https://github.com/pingcap/tikv/issues/1077).

| CF      | RocksDB Key             | RocksDB Value |
| :------ | :---------------------- | :------------ |
| Lock    | `user_key`              | `lock_info`   |
| Default | `{user_key}{start_ts}`  | `user_value`  |
| Write   | `{user_key}{commit_ts}` | `write_info`  |

其中：

- 为了消除歧义，约定 User Key (`user_key`) 指 TiKV Client（如 TiDB）所写入的或所要读取的 Key，User Value (`user_value`) 指 User Key 对应的 Value。
- `lock_info`包含 lock type、primary key、timestamp、ttl 等信息，见 [`src/storage/mvcc/lock.rs`](https://github.com/tikv/tikv/blob/1924a32376b7823c3faa0795f53e836e65eb9ff0/src/storage/mvcc/lock.rs)。
- `write_info`包含 write type、start_ts 等信息，见 [`src/storage/mvcc/write.rs`](https://github.com/tikv/tikv/blob/1924a32376b7823c3faa0795f53e836e65eb9ff0/src/storage/mvcc/write.rs)。







### **transaction.go 代码编写**

首先，编写位于文件 kv/transaction/mvcc/transaction.go 中的代码，将事务支持部分补全。

该部分主要涉及到 Key-Value，Locks 和 Writes 的读取、保存与删除操作，使用的数据结构有 `MvccTxn` 事务、`Lock` 以及 `Write` 等， `MvccTxn` 的成员如下：

```go
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}
```

其中，`StartTS` 表示事务的开始时间戳，使用 `Reader` 可以从数据库中读取所需要的的数据（方法 `GetCF` 以及 方法 `IterCF`），`writes` 中保存的是修改序列，包括 put 以及 delete 两种修改序列的内容。

**以 `PutValue` 以及 `DeleteLock` 为例，使用 `Modify` 将修改数据添加到 `write` 序列中，其中最重要的是，`key` 以及 `Value` 的组织形式以及 Cf 的类别，具体如下表，使用方法 `EncodeKey` 进行 key 的编码：**

| CF      | DB Key                  | DB Value     |
| :------ | :---------------------- | :----------- |
| Lock    | `user_key`              | `lock_info`  |
| Default | `{user_key}{start_ts}`  | `user_value` |
| Write   | `{user_key}{commit_ts}` | `write_info` |

```go
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	}
	txn.writes = append(txn.writes, modify)
}

func (txn *MvccTxn) DeleteLock(key []byte) {
	modify := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, modify)
}
```

读取函数 `GetXXX` 形式的函数以及 `CurrentWrite` 与 `MostRecentWrite` 方法，需要使用 `Reader` 从数据库中读取数据，需要灵活使用 `GetCF` 以及 `IterCF `方法，使用点读或者迭代器的方式读取需要的数据，具体的读取过程如下：

1. `GetLock` 实现比较简单，直接使用 `GetCF` 从数据库中读取出对应的数据即可，需要注意的是，lock 数据属于的列族为 `CfLock` ，具体参见上述的表格；然后将取出的 字节流数据 使用方法 `ParseLock` 格式化即可，简化的读取流程如下，需要判断的步骤省略。

   ```go
   func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
   	lock, err := txn.Reader.GetCF(engine_util.CfLock, key)
   	.......
   	parseLock, err := ParseLock(lock)
   	......
   	return parseLock, nil
   }
   ```

2. `GetValue` 的作用是获取最新的可读数据，其实现比较复杂，该方法使用 `key` 进行查找，**需要返回 `MvccTxn` 开始时间戳 `StartTS` 所代表时间可以读到的最新数据**，具体的实现如下：

   | CF      | DB Key                  | DB Value     |
   | :------ | :---------------------- | :----------- |
   | Lock    | `user_key`              | `lock_info`  |
   | Default | `{user_key}{start_ts}`  | `user_value` |
   | Write   | `{user_key}{commit_ts}` | `write_info` |

   根据数据的存储格式，**当数据提交 Commit 完成后，使用 key 以及 commit_ts 保存 Write 表示其提交的时间，然后之前 Prewrite 提交 Default 的数据是真正的用户数据**。

   因此，需要读取用户数据，需要先**使用 key 以及 StartTS 读取 Write 数据**，然后根据 write 中**取出的 start_ts 查找对应的用户数据**。

   |           | default                        | lock                                  | write                           |
   | :-------- | :----------------------------- | :------------------------------------ | :------------------------------ |
   | **key**   | z{encoded_key}{start_ts(desc)} | z{encoded_key}                        | z{encoded_key}{commit_ts(desc)} |
   | **value** | {value}                        | {flag}{primary_key}{start_ts(varint)} | {flag}{start_ts(varint)}        |

   PS. 需要注意的是，由于是将数据从 数据库中取出单独操作，建议使用如下方式取出 key ，不要使用方法 `Key`  进行直接操作，以免出现异常情况；此外，如果 `write.Kind == WriteKindDelete` 即表示最新数据被删除，则直接返回 nil 即可（**非必须，但是可以提高访问效率**）。

   ```go
   	item := iter.Item()
   	gotKey := item.KeyCopy(nil)
   	userKey := DecodeUserKey(gotKey)
   	
   	value, err := item.ValueCopy(nil)
   	write, err := ParseWrite(value)
   ```

   ```go
   func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
   	iter := txn.Reader.IterCF(engine_util.CfWrite)
   	defer iter.Close()
   	iter.Seek(EncodeKey(key, txn.StartTS))
   	......
   	write, err := ParseWrite(value)
   	......
   	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
   }
   ```

3. `CurrentWrite` 以及 `MostRecentWrite` 方法是读取 write 相关的写入数据，具体的作用以及实现如下：

   `CurrentWrite` 使用 `MvccTxn` 事务开始时间戳 `StartTS` 查找并返回符合要求的 write，**当 write 的 `StartTS` 与 `MvccTxn` 事务开始时间戳 `StartTS` 相同，且 key 值为查找的 key 即为需要的 write**，**该方法的作用是进行回滚 `Rollback` 的时候，查找对应开始时间的时间戳进行删除等操作**。

   返回的值为 write、提交时间戳 commit_ts 以及 error.

   ```go
   func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
   	iter := txn.Reader.IterCF(engine_util.CfWrite)
   	defer iter.Close()
   
   	for iter.Seek(EncodeKey(key, TsMax)); iter.Valid(); iter.Next() {
   		......
   		if write.StartTS == txn.StartTS {
   			return write, decodeTimestamp(gotKey), nil
   		}
   	}
   	return nil, 0, nil
   }
   ```

    `MostRecentWrite` 方法获取的是最新的 write 数据，由于 key 编码的特性（key升序，timestamp降序），所以只需要使用  `iter.Seek(EncodeKey(key, TsMax)) ` 即可找到对应的 write（不使用 `GetCF` 的原因主要是 **无法获取 `commit_ts`** ）。

   ```go
   func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
   	iter := txn.Reader.IterCF(engine_util.CfWrite)
   	defer iter.Close()
   	iter.Seek(EncodeKey(key, TsMax))
   	
   	item := iter.Item()
   	gotKey := item.KeyCopy(nil)
   	userKey := DecodeUserKey(gotKey)
   	if !bytes.Equal(key, userKey) {
   		return nil, 0, nil
   	}
   	value, err := item.ValueCopy(nil)
   	if err != nil {
   		return nil, 0, err
   	}
   	write, err := ParseWrite(value)
   	if err != nil {
   		return nil, 0, err
   	}
   	return write, decodeTimestamp(gotKey), nil
   }
   ```

### **server.go 与 scanner.go 代码编写**

这部分主要实现的 mvcc 相关的一系列操作，包括 get、scan、prewrite、commit 等等方法，具体的实现如下：

这部分的实现主要参考文章 [TiKV 源码解析系列文章（十二）分布式事务](https://pingcap.com/zh/blog/tikv-source-code-reading-12) 以及文章 [TiKV 事务模型概览，Google Spanner 开源实现](https://pingcap.com/zh/blog/tidb-transaction-model)，详细内容详见文章，本次不过多阐述内容，主要讲解实现过程。

#### `KvGet` 函数

该方法的作用是 **读取对应版本的数据**，先查看函数的 **参数** 与 **返回值**，参数为 `GetRequest` 返回值为 `GetResponse`，其定义如下，函数实现基本步骤如下：

```go
type GetRequest struct {
	Context              *Context 
	Key                  []byte   
	Version              uint64  
}
type GetResponse struct {
	RegionError *errorpb.Error 
	Error       *KeyError     
	Value       []byte 
    NotFound     bool     
}
```

1. 使用 `GetRequest` 的 `Context` 创建读取器，然后使用 Reader 以及 ts（Version）创建 MvccTxn，该步骤是该部分所有方法的基本步骤，其他方法可以复用此部分代码，代码如下：

   ```go
   	getResponse := &kvrpcpb.GetResponse{}
   	reader, err := server.storage.Reader(req.Context)
   	if err != nil {
   		if regionErr, ok := err.(*raft_storage.RegionError); ok {
   			getResponse.RegionError = regionErr.RequestErr
   		}
   		return getResponse, err
   	}
   	defer reader.Close()
   	txn := mvcc.NewMvccTxn(reader, req.Version)
   ```

2. 获取 key 值的锁，**如果锁的 TS 小于请求的 Version表示发生冲突，不能返回当前值**，以防止出现幻读的现象，实现的代码如下：

   ```go
   	lock, err := txn.GetLock(req.Key)
   	......
   	if lock != nil && req.Version >= lock.Ts {
   		getResponse.Error = &kvrpcpb.KeyError{
   			Locked: &kvrpcpb.LockInfo{
   				PrimaryLock: lock.Primary,
   				LockVersion: lock.Ts,
   				Key:         req.Key,
   				LockTtl:     lock.Ttl,
   			}}
   		return getResponse, nil
   	}
   ```

3. 如果不存在锁，或者锁的 TS 大于当前的 Version 则可以使用方法 `GetValue` 返回需要的值，将其封装为 `GetResponse` 即可，需要注意的是，**如果 value 为空值则需要将 `NotFound` 置为 true**。

   ```go
   	value, err := txn.GetValue(req.Key)
   	......
   	if value == nil {
   		getResponse.NotFound = true
   	}
   	getResponse.Value = value
   	return getResponse, nil
   ```

#### `KvPrewrite` 函数

事务的提交是一个两阶段提交的过程，第一步是 prewrite，**即将此事务涉及写入的所有 key 上锁并写入 value**，`PrewriteRequest` 请求中会带上事务的 `start_ts` 和选取的 `primary key`。

```go
type PrewriteRequest struct {
	Context   *Context   
	Mutations []*Mutation 
	PrimaryLock          []byte   
	StartVersion         uint64   
	LockTtl              uint64   
}
type Mutation struct {
	Op                   Op      
	Key                  []byte  
	Value                []byte  
}
type Op int32
const (
	Op_Put      Op = 0
	Op_Del      Op = 1
    // Used by TinySQL but not TinyKV.
	Op_Rollback Op = 2
	Op_Lock 	Op = 3 
)
```

如上所示，在 prewrite 时，我们用 `Mutation` 来表示每一个 key 的写入。

**`Mutation`分为 `Put`，`Delete`，`RollBack`和 `Lock`四种类型。**

`Put` 即对该 key 写入一个 value，`Delete`即删除这个 key。`RollBack`与 `Lock` 不在 TinyKV 中使用，因此只处理前两种操作即可，接下来我们创建一个 `MvccTxn`的对象（该部分与 `KvGet` 方法实现相似，详见 `KvGet`），并对每一个 `Mutation` 进行处理，具体的处理过程如下：

```go
type KeyError struct {
	Locked               *LockInfo    
	Retryable            string        
	Abort                string        
	Conflict             *WriteConflict
}
```

`prewrite` 的第一步是 **冲突检查**，查看是否存在更新的写入 write，如果存在更新的写入则表示该修改是无效的，需要返回 `Conflict`。第二步是 **锁检查**，如果存在其他客户端的写入锁，则表示存在锁冲突错误，需要返回响应的锁信息，具体见代码。

如果不存在以上问题冲突，**则可以根据 `OP` 操作的类型，写入锁和数据**，写入操作会被缓存在 `writes`字段中。

```go
	var keyErrors []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(m.Key)
		......
		if write != nil && req.StartVersion <= ts {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,	Key: m.Key,
					Primary:    req.PrimaryLock,
				}})
			continue
		}
        
		lock, err := txn.GetLock(m.Key)
		.......
		if lock != nil && lock.Ts != req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         m.Key,
					LockTtl:     lock.Ttl,
				}})
			continue
		}
        
		var kind mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(m.Key)
		default:return nil, nil
		}
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind})
	}
```

最后，如果存在错误信息，则将错误信息返回；如果不存在错误信息，则将 事务 的 `writes` 字段保存到存储中，提交到数据库中，代码如下所示：

```go
	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	.....
	return resp, nil
}
```

#### `KvCommit` 函数

当 prewrite 全部完成时，client 便会取得 `commit_ts`，然后继续两阶段提交的第二阶段。

`KvCommit`要做的事情很简单，就是把写在 `CF_LOCK` 中的锁删掉，用 `commit_ts`在 `CF_WRITE`写入事务提交的记录，不过出于种种考虑，我们实际的实现还做了很多额外的检查。

```go
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
    
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		......
		if lock == nil {
			continue
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			write, _, err := txn.CurrentWrite(key)
			.......
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{Retryable: "false"}
				}
			}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	......
	return resp, nil
```

正常情况下，该 key 应当存在同一个事务的锁（具有相同的开始时间戳），如果确实如此，则继续后面的写操作即可；否则的话，调用 `CurrentWrite`找到 `start_ts`与当前事务的 `start_ts`相等的提交记录。

有如下几种可能：

1. **该 key 已经成功提交（锁为空，已经被删除了）**。比如，当网络原因导致客户端没能收到提交成功的响应、因而发起重试时，可能会发生这种情况。此外，锁可能被另一个遇到锁的事务抢先提交，这样的话也会发生这种情况。在这种情况下，会走向上面代码的分支，**不进行任何操作返回成功**。
2. 该事务被回滚。比如，如果由于网络原因迟迟不能成功提交，直到锁 TTL 超时时，事务便有可能被其它事务回滚，这时需要使用 `CurrentWrite`找到 `start_ts`与当前事务的 `start_ts`相等的提交记录已查看是否已经被 回滚，如果被回滚表示已经被废弃，不需要重试，否则进行重试。

#### `KvCheckTxnStatus` 函数

```go
type CheckTxnStatusRequest struct {
	Context              *Context 
	PrimaryKey           []byte  
	LockTs               uint64   
	CurrentTs            uint64   
}
type CheckTxnStatusResponse struct {
	RegionError *errorpb.Error
	// Three kinds of txn status:
	// locked: lock_ttl > 0
	// committed: commit_version > 0
	// rolled back: lock_ttl == 0 && commit_version == 0
	LockTtl       uint64 
	CommitVersion uint64 
	// The action performed by TinyKV in response to the CheckTxnStatus request.
	Action        Action
}
```

`KvCheckTxnStatus` 检查超时，删除过期的锁并返回锁的状态。

如上所示，`CheckTxnStatusResponse` 需要对三种不同的状态返回相应的数据，当数据仍存在锁且锁未超时时，返回 `lock_ttl > 0`；当锁不存在时，则需要检查是否已经提交 Commit，如果已经提交且写入不是 WriteKindRollback 类型，则返回 `commit_version > 0`；其他情况都是存在问题的情况，需要让返回的 `lock_ttl == 0 && commit_version == 0`。

```go
	lock, err := txn.GetLock(req.PrimaryKey)
	......
	if lock == nil {
		write, ts, err := txn.CurrentWrite(req.PrimaryKey)
		......
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.CommitVersion = ts
			}
			return resp, nil
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		......
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
```

需要注意的是，如上所示，如果 **锁为空**，但是不存在写入（或者写入得到类型是 RollBack 类型），则将 `resp.Action` 置为 `Action_LockNotExistRollback` ，表示需要回退。

```go
	resp.LockTtl = lock.Ttl
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,	Kind: mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		......
        resp.LockTtl = 0
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, nil
```

如上所示，如果锁存在，但是未超时，则需要将 LockTtl 置为对应的 ttl；如果超时了，**则需要删除对应的锁以及用户数据**，并进行回滚，并将 `resp.Action` 置为 `Action_TTLExpireRollback` 。

#### `KvBatchRollback` 函数

```go
type BatchRollbackRequest struct {
	Context              *Context 
	StartVersion         uint64  
	Keys                 [][]byte 
}
```

对于存在多个 region 的请求，当某个 `prewrite` 请求失败，则需要将所有的  `prewrite` 全部回滚，以保证事务数据的原子性，`BatchRollbackRequest` 请求的个数如上所示，**需要使用 Keys 以及 StartVersion 查找对应的锁以及用户数据进行清除**。

需要注意的是，当 write 存在，则表示该事务存在已经提交的部分，该回滚存在问题，回滚中断，返回 `kvrpcpb.KeyError{Abort: "true"}` 表示回滚中断（该部分为测试要求，具体作用不知）。

```go
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		......
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		......
        txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
```

然后，获取锁，如果不存在锁或者锁的版本与回滚的版本不同，表示当前锁已经被回滚，则仅需要做标记即可，否则需要删除锁与用户数据，同时做标记。

#### `KvResolveLock` 函数

```go
// Resolvelock will find locks belonging to the transaction with given start timestamp.
// If commit_version is 0, TinyKV will rollback all locks. 
// If commit_version is > 0 it will commit those locks with the given commit timestamp.
// The client will make a resolve lock request for all secondary keys once it has successfully committed or rolled back the primary key.
type ResolveLockRequest struct {
	Context              *Context 
	StartVersion         uint64   
	CommitVersion        uint64   
}
```

如上所示，`KvResolveLock` 函数的功能是找到属于这个事务的所有锁进行回滚或者提交，如果所给的 CommitVersion 大于0表示需要提交，否则回滚。该方法通常用于 secondary keys 的处理，具体处理流程如下：

首先，需要使用 `StartVersion` 查找锁对应的 keys 用于提交或回滚。

```go
  	iter := reader.IterCF(engine_util.CfLock)
	.......
	defer iter.Close()

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
        ...
		lock, err := mvcc.ParseLock(value)
        ...
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
```

然后，根据 CommitVersion 回滚或者提交。

```go
	// If commit_version is 0, TinyKV will rollback all locks.
	// else it will commit those locks with the given commit timestamp.
	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	} else {
		resp1, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	}
```

#### `KvScan` 函数

该函数的主要作用是从 `StartKey` 开始读取最多 `Limit` 个版本为 `Version` 的用户数据。

该部分的实现需要完成 scanner.go 部分的辅助数据结构以及方法，其实现参考 Reader的实现。

```go
type ScanRequest struct {
	Context  *Context 
	StartKey []byte  
	Limit                uint32  
	Version              uint64  
}
type ScanResponse struct {
	RegionError *errorpb.Error 
	Pairs  []*KvPair     // Other errors are recorded for each key in pairs.
}
type KvPair struct {
	Error                *KeyError 
	Key                  []byte    
	Value                []byte  
}
```

`Scanner` 需要实现的方法主要是 `Next` 以及 `Close` 方法，scanner 主要的作用是通过调用 next 返回下一个符合要求的 用户数据，以及键值 key，因此 scanner 至少需要包含一个 迭代器，以及读取数据的事务 `txn`，实现的代码如下：

创建scanner（包含迭代器）时，需要将迭代器定位到第一个可以读取的键值 `EncodeKey(startKey, scanner.txn.StartTS)`

进行迭代时，需要注意的是，需要跳过所有统一键值 key 不同版本 ts 的所有数据，**如果当前键值不存在当前 ts 版本可读的数据（请求的时间戳过早，早于所有版本的时间戳）**，则需要将直接返回下一个键值的数据。

```go
type Scanner struct {
	txn      *MvccTxn
	iter     engine_util.DBIterator
}

func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	scanner := &Scanner{
		txn:    		 txn,
		iter:   		 txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, scanner.txn.StartTS))
	return scanner
}

func (scan *Scanner) Next() ([]byte, []byte, error) {
	if !scan.iter.Valid(){
		return nil, nil, nil
	}
	item := scan.iter.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)

	scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
	if !scan.iter.Valid(){
		return nil, nil, nil
	}
	item = scan.iter.Item()
	gotKey = item.KeyCopy(nil)
	key := DecodeUserKey(gotKey)
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return userKey, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return userKey, nil, err
	}
	var value []byte
	if write.Kind == WriteKindDelete {
		value = nil
	}
	value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if !bytes.Equal(key, userKey) {
		return scan.Next()
	}
	for {
		if !bytes.Equal(key, userKey) {
			break
		}
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		tempItem := scan.iter.Item()
		gotKey = tempItem.KeyCopy(nil)
		key = DecodeUserKey(gotKey)
	}
	return userKey, value, err
}


```

参照 rawscan 的写法，具体实现如下，注意进行错误处理，当存在锁冲突时，需要进行错误返回。

```go

    scanner := mvcc.NewScanner(req.StartKey, txn)
    defer scanner.Close()

    var pairs []*kvrpcpb.KvPair
    limit := int(req.Limit)
    for i := 0; i < limit; {
        key, value, err := scanner.Next()
        .....
        if key == nil {
            break
        }

        lock, err := txn.GetLock(key)
        ......
        if lock != nil && req.Version >= lock.Ts {
            pairs = append(pairs, &kvrpcpb.KvPair{
                Error: &kvrpcpb.KeyError{
                    Locked: &kvrpcpb.LockInfo{
                        PrimaryLock: lock.Primary,
                        LockVersion: lock.Ts,
                        Key:         key,
                        LockTtl:     lock.Ttl,
                    }},
                Key: key,
            })
            i++
            continue
        }
        if value != nil {
            pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
            i++
        }
    }
    resp.Pairs = pairs
    return resp, nil
}
```
