# LogX (?)

LogX (still soliciting a more cool name) is a framework to build scalable data streaming pipeline with ease.
It is designed to work with different sources and sinks, and with the help of Spark (although not required),
it can support structured schema and perform in-stream transformations at different levels of complexity:
 1. Simple record level transformation (also called map in other context), filtering, splitting and merging
 2. Windowed or accumulated aggregation
 3. Joining

The framework also provides the structure to implement robust checkpointing and flexible instrumentation.

To run a simple demo from command line (need to install sbt first):

`sbt test`

It should print traces to the console.