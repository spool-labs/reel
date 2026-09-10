# tape-reel-mock

A test double for the `Store` trait, not a backend choice. Column families
are hash maps under one lock over the whole store, and every read copies its
value: the shape an oracle wants and the wrong one for production. Its
copying is deliberate, so a differential test against the real engine reads
obviously.

Not published. If you want a store that serves hot values from memory, that
is the reel engine itself with a carried budget armed.
