# tape-reel-cli

The operator toolbox for reel volumes. One binary, `reel`, six verbs:

```
reel <VOLUME> [--column NAME:ID[:WIDTH]]... [--paged]
              [-o text|json|markdown] [--color auto|always|never] <VERB>

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
flags. Cells the chosen open cannot fill print `-`, never a false zero.

## What a report looks like

Head, then the answer, then the figures behind it:

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

A figure this open could not count comes back beside a caveat saying so,
under `NOT COUNTED`, with the flag that would answer it:

```
NOT COUNTED
● no columns declared, so sealed spans and standing covers count nothing
  → pass --column NAME:ID for each column the volume was written with
```

Nothing is ever dropped silently. A listing that truncates says what it
truncated and how to see the rest, faults included; segments that swept clean
and empty are counted in the caption rather than given a row of zeroes each.
Totals are never taken from a truncated listing: `--limit` shortens the rows
and never the figures above them.

## Formats

`-o text` is for a person, `-o json` for a script or an agent, `-o markdown`
for a pull request, an issue, or a CI job summary.

Frames and colour are drawn only where the output is a terminal. A pipe, a
file and a CI log get the plain form, `NO_COLOR` is honoured, and `--color`
overrides both ways.

**Json is the complete record.** It carries every row a text listing
truncates, and every caveat the text form renders as a note, as a
`caveats` array of `{what, fix}`. A consumer never has to parse prose to
learn that a figure is a floor rather than a total.

The `verify` sweep can run for minutes, so it draws a progress bar on
standard error where somebody is watching, and erases it before the report
is written. A redirected sweep draws nothing, so `reel vol verify > out` and
`reel vol -o json verify | jq` both arrive clean.

## The layer behind it

The reports themselves are `reel::report`, not this binary. `report::cue`
answers a `CueReport`; the report says its own shape as a `report::doc::Doc`
of head, verdict, facts, tables, notes and footer; `report::render::text`
and `report::render::markdown` draw that shape, and serde serialises the
struct. A crate embedding the engine gets the same rows without a command
line library or a copied format string.

Adding a format is a module beside those two rather than another set of
format strings per report, and a report that grows a block gets it in every
format at once. Column widths are measured off the content, so nothing
declares a layout.

`report::spec` parses the same `PATH[:capacity][:dead]` and
`NAME:ID[:WIDTH]` strings whatever the frontend parses arguments with. What
belongs to the frontend and not the engine is deciding whether there is a
terminal out there: `render::Style` is handed down, never discovered.
