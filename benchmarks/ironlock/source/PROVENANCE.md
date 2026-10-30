# Provenance

Copied verbatim (`cp -p`, 2026-10-28) from the Ironclad repository checked out at
`plan/benchmarks/related-work-papers/sources/ironclad/`, commit 2fe4dcdc323b92e93f759cc3e373521366b7f691 (2023-06-02, https://github.com/microsoft/Ironclad.git). The paths below are
relative to `ironclad/ironfleet/src/`; the directory structure is kept.

| file here | path in the source tree |
|---|---|
| `Dafny/Distributed/Impl/Lock/CmdLineParser.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/CmdLineParser.i.dfy` |
| `Dafny/Distributed/Impl/Lock/Host.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/Host.i.dfy` |
| `Dafny/Distributed/Impl/Lock/Message.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/Message.i.dfy` |
| `Dafny/Distributed/Impl/Lock/NetLock.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/NetLock.i.dfy` |
| `Dafny/Distributed/Impl/Lock/Node.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/Node.i.dfy` |
| `Dafny/Distributed/Impl/Lock/NodeImpl.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/NodeImpl.i.dfy` |
| `Dafny/Distributed/Impl/Lock/PacketParsing.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Impl/Lock/PacketParsing.i.dfy` |
| `Dafny/Distributed/Protocol/Lock/Node.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Protocol/Lock/Node.i.dfy` |
| `Dafny/Distributed/Protocol/Lock/RefinementProof/DistributedSystem.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Protocol/Lock/RefinementProof/DistributedSystem.i.dfy` |
| `Dafny/Distributed/Protocol/Lock/RefinementProof/Refinement.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Protocol/Lock/RefinementProof/Refinement.i.dfy` |
| `Dafny/Distributed/Protocol/Lock/RefinementProof/RefinementProof.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Protocol/Lock/RefinementProof/RefinementProof.i.dfy` |
| `Dafny/Distributed/Protocol/Lock/Types.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Protocol/Lock/Types.i.dfy` |
| `Dafny/Distributed/Services/Lock/AbstractService.s.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Services/Lock/AbstractService.s.dfy` |
| `Dafny/Distributed/Services/Lock/LockDistributedSystem.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Services/Lock/LockDistributedSystem.i.dfy` |
| `Dafny/Distributed/Services/Lock/Main.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Services/Lock/Main.i.dfy` |
| `Dafny/Distributed/Services/Lock/Marshall.i.dfy` | `ironclad/ironfleet/src/Dafny/Distributed/Services/Lock/Marshall.i.dfy` |
| `IronLockServer/Program.cs` | `ironclad/ironfleet/src/IronLockServer/Program.cs` |


Roles: `Services/Lock/AbstractService.s.dfy` is the specification (`ServiceState'(hosts, history)`,
`Service_Correspondence`); `Protocol/Lock/Node.i.dfy` and `Types.i.dfy` are the protocol
(`NodeInit`, `NodeGrant`, `NodeAccept`; `Transfer`/`Locked`); `Impl/Lock/` is the executable
Dafny implementation; `Protocol/Lock/RefinementProof/` and `Services/Lock/{Main,LockDistributedSystem,Marshall}.i.dfy`
the proof; `IronLockServer/Program.cs` the C# driver. Not copied: `Services/Lock/.gitignore`, the
IronLockServer project files, and the included common framework (`Common/Framework/*`, `Common/Native/*`).

Not modified. The Rust and Java programs in `../rust/` and `../java/` are translations, described in `../README.md`.
