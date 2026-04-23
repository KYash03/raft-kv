# raft-kv

raft consensus in go, with a tiny linearizable in memory kv store on top.
nodes talk to each other over grpc.

mostly follows the raft paper (ongaro & ousterhout, USENIX ATC '14,
https://raft.github.io/raft.pdf).

## running

```
brew install go protobuf
go build ./...

# 3 node cluster on localhost
./raftkv --id=1 --addr=:9001 --peers=1@:9001,2@:9002,3@:9003 &
./raftkv --id=2 --addr=:9002 --peers=1@:9001,2@:9002,3@:9003 &
./raftkv --id=3 --addr=:9003 --peers=1@:9001,2@:9002,3@:9003 &
```

hit any node with grpcurl. if it isn't the leader it'll send back a
`leader_hint` you can retry against.

## tests

```
go test ./... -race
```

## TODO

* persistence
* read index for Get
* snapshotting
* cluster membership changes
