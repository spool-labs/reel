# tape-reel-core

A thin key-value store abstraction: byte-oriented access through the `Store` trait, write batches,
and ordered iteration. Backends implement `Store`, and the reel engine is the reference
implementation.

This crate has no engine in it. Depend on it to write code that is generic over the store, or to
implement the trait for your own backend.
