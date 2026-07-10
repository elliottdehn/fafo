I made a silly (but real) database.

I got tired of dealing with shards, so I set out upon building a database that shards itself.

After some design... misdirections, I settled upon one concept: the database as an object.

Most designs treat the database instance as precious; this one treats instances as profane.

SQLite is one of the most well-tested, robust pieces of software on earth. I leveraged that.

S3 and R2 are two of the most scalable file storage surface areas on earth. I leveraged them.

Features:

- Almost 80,000 optimistic writes per second for a single database, better than Postgres.
- Spinning up a new database takes one API call, a few microseconds, and costs nothing.
- **ACID transactions ACROSS databases. Declare the objects you're touching up front, commit atomically.**
- It learns its own sharding. Objects migrate toward the workers that transact on them together.
- Roughly $4 per billion writes, and a database nobody is using costs... the bytes it occupies.

How it works, in one paragraph: every object is its own SQLite file. One worker owns an object at a time, so writes are serial and there are no locks to take. Durable truth lives in object storage; the local file is a disposable working copy. A write applies locally in microseconds, rides the next boat to R2, and the commit record is a single blob write — atomic across every database in the transaction. Cold object? Activate it from the blob on demand. Disk pressure? Evict it — it was never precious. Ownership moves by learning from the transaction graph.

The honest fine print: optimistic writes can lose the last in-flight boat if you crash inside a storage round trip — that's the contract, and a pessimistic transaction is your durability barrier whenever you care. One white-hot object is bounded by one core (spread your load across objects; that's the whole idea). There's no million-client fanout tier. And there are no global secondary indexes — but let's be honest, at scale there never were. There was always either a scatter-gather or an eventually-consistent materialized view wearing an index costume. Here you build the view yourself with an atomic outbox, and the invoice is printed in the participant list where you can see it.

It's ~7,000 lines of Rust, MIT licensed, and running in production for an audience of one: https://github.com/elliottdehn/fafo

Honestly, that's not bad. I'm pretty happy with that.

Build it try it (Rust toolchain required): `git clone https://github.com/elliottdehn/fafo && cd fafo && ./fafo up`
