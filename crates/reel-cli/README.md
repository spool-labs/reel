# reel-cli

The operator toolbox for reel volumes. One binary, `reel`, four verbs:

```
reel <VOLUME> [--column NAME:ID[:WIDTH]]... [--paged] [-o text|json] <VERB>

  cue      sequence, floor, segments live/dead/held, covers, graves, cues held
  stat     per-column runs, records, live and dead bytes, dead share
  verify   offline integrity sweep, exit 1 on any fault
  doctor   machine facts and the bias verdict, opens no volume
```

A volume's schema is not discoverable from disk, so column names arrive as
flags. Cells the chosen open cannot fill print `-`, never a false zero, with
a line naming which open would answer. The verify sweep is driven by the
segment files on disk; a file the index does not name is itself a fault.
