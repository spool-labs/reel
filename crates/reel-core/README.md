# tape-reel-core

A thin key-value store abstraction: byte-oriented access through the `Store`
trait, write batches, and ordered iteration. Backends implement `Store`, and the
reel engine is the reference implementation.

This crate carries no engine. Depend on it to write code that is generic over
the store, or to implement the trait for a backend of your own.
