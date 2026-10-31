# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-31 from
`plan/benchmarks/related-work-papers/sources/igloo-vm/extracted/home/igloo/`, the case studies
extracted from the Igloo artifact VM (Zenodo record 4039826, `igloo-artifact.ova`; download and
extraction record `ARTIFACT-HUNT.md` §8) of Sprenger, Klenze, Eilers, Wolf, Müller, Clochard,
Basin, "Igloo: Soundly Linking Compositional Refinement and Separation Logic for Distributed System
Verification", OOPSLA 2020:

- `python/`: `igloo-python/leader-election/leader-election-python-executable/leader/election.py`
  (`mainLoop`, the code the benchmark follows), `int_socket.py`, `run_election.py`.
- `java/`: `igloo-java/leader-election-sequential/src/impl/Leader.java`, `LocalExampleRun.java`,
  `UDPSocketHelper.java` (the same loop in Java; the VeriFast-verified version).
- `isabelle/`: `igloo-isabelle/case-studies/leader-election/Leader_Election_0.thy` … `_4.thy`,
  `Ring_network.thy` (the refinement chain: `U_0` and `inv_no_multiple_leaders` in `_0`,
  `inv_leader_top` in `_1`, the environment `tr3e` with loss, duplication and reordering in `_3`,
  the `example012` ring in `Ring_network.thy`).

md5: `election.py` `7f01aa8fe2634f990efd5c9ba6dda974`, `Leader.java` `037ec97ea03890f6d488b84a7e68a7c4`,
`Leader_Election_0.thy` `04a03b872a5150f24b26ad6ad932dfbb`, `Leader_Election_1.thy`
`dd3ba8ca35290f14717a2579839b5d07`, `Leader_Election_3.thy` `428c6466ff89448fbcf4f85b8a2d78b8`,
`Ring_network.thy` `61a2b2fb5d9cfa09ecb8dd29d4a61e94` (the rest: `md5sum source/*/*`).

Catalogue record: `related-work-papers/sources/CATALOGUE-artifacts-2.md`, "IG leader election";
candidate N23 in `CANDIDATES.md` §1b.
