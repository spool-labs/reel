# tape-reel-cli

The `reel` binary inspects, verifies and checkpoints reel volumes. It has six verbs:

```
reel <VOLUME> [--column NAME:ID[:WIDTH]]...
              [-o text|json|markdown] [--color auto|always|never] <VERB>

  cue         sequence, floor, segments live/dead/held, covers, graves, cues held
  stat        per-column runs, records, live and dead bytes, dead share
  spans       sealed segments standing over each column
  verify      offline integrity sweep, exit 1 on any fault
  checkpoint  durable copy into a new directory, takes the ownership lock
  doctor      machine facts and the bias verdict, opens no volume
```

`--volume PATH[:capacity][:dead]` adds another root of the same volume set, in the order the set
was written. A bare path is a fast volume, `:capacity` marks the capacity tier and `:dead` marks a
drive declared dead. A set spanning several roots refuses to open without its full list.

`cue`, `stat`, `spans` and `verify` open the volume read-only and take no lock, so they can read a
volume another process is writing. `checkpoint` seals the tails to fix the point it copies at, so it
takes the ownership lock.

A volume's schema isn't stored on disk, so you declare its columns with `--column`. Only declared
columns are counted.

## What a report looks like

A head, then the answer, then the figures behind it:

```
╭───────────────────────────────────────────────────────────────╮
│ vol · verify · 10 segment files · read-only, nothing written  │
├───────────────────────────────────────────────────────────────┤
│ CLEAN — 88 records, 5.0 MiB, no faults                        │
╰───────────────────────────────────────────────────────────────╯

volume           /srv/reel/vol
records checked  88
bytes checked    5.0 MiB

  segment  kind    records    bytes  faults
  1        sealed       31  1.9 MiB       0
  9        sealed       31  1.9 MiB       0
  all 2 segments holding records; 7 swept clean and empty — --limit 0 to list them

CHECKED      every sealed segment's footer decodes · every record a footer
             indexes matches its checksum · a segment with no footer is walked
             to its write frontier · every segment file on the roots is one the
             index names
NOT CHECKED  versions a footer no longer indexes · whether the segments agree
             with each other
```

A figure the open could not count comes back with a caveat under `NOT COUNTED`, and with the flag
that would answer it:

```
NOT COUNTED
● no columns declared, so sealed spans and standing covers count nothing
  → pass --column NAME:ID for each column the volume was written with
```

Nothing is dropped silently. A truncated listing, faults included, says what it cut and how to see
the rest. Segments that swept clean and empty are counted in the caption, with no row of zeroes
each. Totals never come from a truncated listing: `--limit` shortens the rows and leaves the figures
above them whole.

## Formats

| `-o` | for |
|---|---|
| `text` | a person |
| `json` | a script or an agent |
| `markdown` | a pull request, an issue, or a CI job summary |

Frames and colour appear only when the output is a terminal. A pipe, a file and a CI log get the
plain form. `NO_COLOR` is honoured, and `--color` overrides both ways.

**Json is the complete record.** It holds every row a text listing truncates, and every caveat the
text shows as a note, as a `caveats` array of `{what, fix}`. A consumer never parses prose to learn
whether a figure is a floor or a total.

`verify` can run for minutes, so it draws a progress bar on standard error when someone is watching
and erases it before the report is written. A redirected sweep draws nothing, so
`reel vol verify > out` and `reel vol -o json verify | jq` both come out clean.

## The report layer

The reports live in the engine crate as `reel::report`. `report::cue` returns a `CueReport`. Each
report describes its own shape as a `report::doc::Doc` of head, verdict, facts, tables, notes and
footer. `report::render::text` and `report::render::markdown` draw that shape, and serde serialises
the struct. A crate embedding the engine gets the same rows without a command line library or a
copied format string.

A new format is one module beside those two. A block added to a report shows up in every format at
once. Column widths are measured from the content, so nothing declares a layout.

`report::spec` parses the `PATH[:capacity][:dead]` and `NAME:ID[:WIDTH]` strings, whatever argument
parser the frontend uses. The frontend decides whether a terminal is attached and hands
`render::Style` down.
