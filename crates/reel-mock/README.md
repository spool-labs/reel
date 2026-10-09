# tape-reel-mock

An in-memory test double for the `Store` trait. Column families are hash maps under one lock over
the whole store, and every read copies its value. That suits an oracle and would be wrong for
production. The copying is on purpose, so a differential test against the real engine is easy to
read.

Not published.
