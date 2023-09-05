# Implementation Notes

This document contains some notes on the implementation of GFS in Go.

## GFS Overview

We first give a brief overview of GFS and discuss some of the design decisions.

GFS is a distributed file system. As a file system, it provides an interface for clients to create, read, and write files.

### Store Data in Chunks

GFS splits files into fixed-size *chunks*. Each chunk has multiple *replicas* and are stored on different *chunkservers*. The unique identifier of a chunk is called a *chunk handle*.

Using fixed-size chunks makes it easier to store data distributedly. The drawback is that it is less efficient when storing thousands of small files. However, GFS is designed for large files (and thus it uses 64MB large chunks), so this is not a problem.

### Metadata and Single Master

The metadata of the file system includes:

- File namespaces,
- Mapping from files to chunks, and
- Locations of each chunk's replicas.

These metadata are stored on a single *master* server. A GFS cluster consists of a single master and multiple chunkservers. The fact that the metadata is not coupled with the data allows the seperation of control and data flow.

The single master design simplifies the system and enables the master to make sophisticated chunk operations, but this also creates a single point of failure and a potential bottleneck.

To avoid the disaster when the master fails, GFS uses *operation logs* to store the metadata persistently and replicate the logs on other machines. This is not implemented in this project, so the detail is not discussed here.

To avoid the bottleneck, the client interacts with the master only for metadata operations. The client caches the metadata and interacts with the chunkservers for data operations. The architecture is shown below.

![](assets/Figure1-GFS-Architecture.svg)

#### Discussion: GFS vs Store Chunks using DHT

Another way to store chunks distributedly is to use a distributed hash table (DHT). GFS is different from DHT mainly because of the centralized master. This makes GFS better in the following ways:

- More control over chunk placement: the master in GFS can control the placement of chunks, while DHT cannot since it is decentralized. This also makes GFS more fault tolerant: the master can place replicas on different racks, re-replicate chunks when the number of replicas falls below a threshold, etc.
- Better performance: GFS is faster because it has less routing overhead. A typical task for GFS is to read a large file sequentially. DHT needs to route the request to the correct node, while GFS can directly interact with the correct chunkservers. This also improves the overall network utilization.
- Stronger consistency model: GFS provides stronger consistency guarantees. DHT is eventually consistent, while GFS provides atomic record append. This is discussed in the next section.

### Atomic Record Append

Maintaining consistency when multiple clients are appending to the same file is difficult. GFS provides an atomic record append operation to simplify this task. The operation appends data to the end of a file and returns the offset of the data. GFS guarantees that the
data is written at least once as an atomic unit (i.e., a continuous sequence of bytes). This is achieved by the lease mechanism.

Every mutation operation can be considered as write or append to a chunk. Since there is multiple replicas of each chunk, we need to ensure that the replicas are consistent. GFS uses a *lease* mechanism to achieve this. The master grants a lease to one of the replicas, called the *primary*. Other replicas are called *secondaries*. The primary picks a serial order for all mutations to the chunk. All replicas follow this order when applying mutations. The client interacts with the primary directly, which also minimizes the burden on the master.

## Implementation Details

The framework given in [this lab](https://bitbucket.org/abcdabcd987/ppca-gfs/) was used as a starting point. The graybox test from the original lab was rewritten using the [assert](https://pkg.go.dev/github.com/stretchr/testify/assert) package, making it easier to debug.

### Client Interface

The client interface includes common file system operations: `Create`, `Mkdir`, `List`, `Read`, `Write`, `Append`. Some underlying operations are also exposed: `GetChunkHandle`, `ReadChunk`, `WriteChunk`, `AppendChunk`.

### Tree-Structure Namespace

The original GFS paper uses a flat namespace: a mapping from full pathnames to metadata. This may be simple to implement, but it is not very efficient. For example, listing a directory requires listing all files and filtering out the files in the directory. We may also need prefix compression to reduce the memory usage of storing the pathnames. Therefore, in this implementation, we use a tree-structure namespace instead.

### Locking

To be continued...
