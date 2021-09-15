# **Project1 StandaloneKV（单节点键值存储)**

- 目标：实现一个单节点键值存储（非分布式系统），使用column family。
- 这个存储系统支持四个基本的操作：`Put/Delete/Get/Scan`
- 需要完成两个部分：
  1. 实现单节点的引擎（需要实现`kv/storage/standalone_storage/standalone_storage.go`）
  2. 实现键值存储的处理函数（需要实现`kv/Server/Server.go`的四个基本的函数）

#### 实现单节点的引擎

使用 [badger](https://dgraph.io/docs/badger/get-started/) 的 API 实现`kv/storage/standalone_storage/standalone_storage.go`中的函数，将其封装为 Storage 的两个方法，具体的方法如下（consider the `kvrpcpb.Context` ）：

``` go
type Storage interface {
    // Other stuffs
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

> Hints:
>
> - You should use [badger.Txn]( https://godoc.org/github.com/dgraph-io/badger#Txn ) to implement the `Reader` function because the transaction handler provided by badger could provide a consistent snapshot of the keys and values.
>
>   使用 [badger.Txn]( https://godoc.org/github.com/dgraph-io/badger#Txn ) 实现 Reader 函数，事务将提交具有一致性的键值与数值
>
> - Badger doesn’t give support for column families. engine_util package (`kv/util/engine_util`) simulates column families by adding a prefix to keys. For example, a key `key` that belongs to a specific column family `cf` is stored as `${cf}_${key}`. It wraps `badger` to provide operations with CFs, and also offers many useful helper functions. So you should do all read/write operations through `engine_util` provided methods. Please read `util/engine_util/doc.go` to learn more.
>
>   Badger 不支持 column families，需要使用 (`kv/util/engine_util`) 包中的函数处理 CF 的 Key 值，所有的读写操作都需要使用 `engine_util` 提供的方法
>
> - TinyKV uses a fork of the original version of `badger` with some fix, so just use `github.com/Connor1996/badger` instead of `github.com/dgraph-io/badger`.
>
> - Don’t forget to call `Discard()` for badger.Txn and close all iterators before discarding.
>
>   关闭时需要使用 `Discard()` 关闭所有的迭代器

解决过程：代码`standalone_storage.go`中的函数可以参考`kv/storage/raft_storage/raft_storage.go`中的写法，将结点之间进行同步的部分剔除，即可得到单一结点的函数，`RaftStorage`定义如下：

```go
type RaftStorage struct {
	engines *engine_util.Engines
	config  *config.Config

	node          *raftstore.Node
	snapManager   *snap.SnapManager
	raftRouter    *raftstore.RaftstoreRouter
	raftSystem    *raftstore.Raftstore
	resolveWorker *worker.Worker
	snapWorker    *worker.Worker

	wg sync.WaitGroup
}
```

```go
updates := make(map[string]string)
txn := db.NewTransaction(true)
for k,v := range updates {
  if err := txn.Set([]byte(k),[]byte(v)); err == badger.ErrTxnTooBig {
    _ = txn.Commit()
    txn = db.NewTransaction(true)
    _ = txn.Set([]byte(k),[]byte(v))
  }
}
_ = txn.Commit()
```



#### 实现服务器的处理函数

The final step of this project is to use the implemented storage engine to build raw key/value service handlers including **RawGet/ RawScan/ RawPut/ RawDelete**. The handler is already defined for you, you only need to **fill up the implementation in `kv/server/server.go`**. Once done, remember to run `make project1` to pass the test suite.

首先写RawGet函数，传入**`RawGetRequest`**使用其**`Context`**进行查询，返回其数值组成`RawGetResponse`

```go
type RawGetRequest struct {
	Context              *Context 
	Key                  []byte  
	Cf                   string   
	XXX_NoUnkeyedLiteral struct{}
	XXX_unrecognized     []byte  
	XXX_sizecache        int32   
}

type RawGetResponse struct {
	RegionError *errorpb.Error 
	Error       string         
	Value       []byte         
	// True if the requested key doesn't exist; another error will not be signalled.
	NotFound             bool     
	XXX_NoUnkeyedLiteral struct{}
	XXX_unrecognized     []byte  
	XXX_sizecache        int32 
}
```















































