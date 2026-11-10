# Generators of the scale-up rows of 2026-11-10 (cluster/campaign/scaleup-1110/, plan/eval2/SCALEUP-2026-11-10.md)
Written by the scale-up agent in /tmp/su1110/ and archived here as they were (R4: every row reproducible from a script).
an.py builds the inventory from the twin and real-software CSVs and brain-data; gen.py chooses the next sizes, estimates
their cost and writes the rows files; md.py writes the plan's tables; doc.py and patch2.py are the agent's helpers. The
intermediate pickles stayed in /tmp. Afterwards the coordinator capped the JVM heaps in the rows files (see their line 2).
