# reel-cli

The operator toolbox for reel volumes. One binary, `reel`, six verbs:

```
reel <VOLUME> [--column NAME:ID[:WIDTH]]... [--paged] [-o text|json] <VERB>

  cue         sequence, floor, segments live/dead/held, covers, graves, cues held
  stat        per-column runs, records, live and dead bytes, dead share
  spans       sealed segments standing over each column
  verify      offline integrity sweep, exit 1 on any fault
  checkpoint  durable copy into a new directory, takes the ownership lock
  doctor      machine facts and the bias verdict, opens no volume
```

Every verb but `checkpoint` opens read-only and lockless, so it reads a
volume something else is writing; `checkpoint` seals the tails to draw the
line it copies at, so it takes the lock.

A volume's schema is not discoverable from disk, so column names arrive as
flags. Cells the chosen open cannot fill print `-`, never a false zero, with
a line naming which open would answer. The verify sweep is driven by the
segment files on disk; a file the index does not name is itself a fault.

The reports themselves are `reel::report`, not this binary: `report::cue`
answers a `CueReport`, `report::text` renders it, and serde serialises it. A
crate embedding the engine gets the same rows without a command line library
or a copied format string, and `report::spec` parses the same
`PATH[:capacity][:dead]` and `NAME:ID[:WIDTH]` strings whatever it parses
arguments with.
