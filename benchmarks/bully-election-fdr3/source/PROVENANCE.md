# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-31 from `plan/benchmarks/related-work-papers/sources/fdr3-inputs/`:

- `bully.7.csp` (335 lines, md5 `28b4e3471477ca3fe3ee7e4a8f98bf57`) from `tacas14/Samples/`: the
  input set of Gibson-Robinson, Armstrong, Boulgakov, Roscoe, "FDR3 — A Modern Refinement
  Checker for CSP", TACAS 2014 (`https://www.cs.ox.ac.uk/files/6231/Samples.zip`, md5
  `d135693d59a45b4f30281551e312932b`). Bill Roscoe's model of the bully algorithm
  (Understanding Concurrent Systems, ch. 14); its active assertion is `assert not AGREEBY(0,Proc)
  [T= Network\diff(Events,AlphaAgree)`: the specification fails (the buggy version).
- `bully.8.csp` (370 lines, md5 `6a0ad4f806c57913afc10838a1112c41`) from `cloud14/Paper Scripts/`:
  the input set of the later "FDR into The Cloud" paper
  (`http://www.cs.ox.ac.uk/files/6643/Samples.zip`): "corrected version", N = 8, active assertion
  `assert AGREEBY(0,Proc) [T= Network\diff(Events,AlphaAgree)` ("all the above specifications now
  succeed").

Catalogue record: `related-work-papers/sources/CATALOGUE-artifacts-2.md`, "FDR/bully.7.csp";
candidate N24 in `CANDIDATES.md` §1b; download record `ARTIFACT-HUNT.md` §7.
