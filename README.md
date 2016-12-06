# Blink

Blink's superpower is to cast portals in the blink of an eye. A portal casted by blink is a one-way gate across space-time,
which pulls data from some source location (potentially data from the past), and push to another sink location.
The purpose of the blink framework is to build scalable data streaming pipelines with ease, by creating a network of diverse portals as needed.
It is designed to work with different sources and sinks, and with the help of Spark (although not required),
it can support structured schema and perform in-stream transformations at different levels of complexity:
 1. Simple record level transformation (also called map in other context), filtering, splitting and merging
 2. Windowed or accumulated aggregation
 3. Joining

The framework also provides the structure to implement robust checkpointing.

The instrumentation API can be used to build power portal network monitor and control center.

At the high level, blink has 3 layers.
 1. The most abstract layer performs a portal control loop that pulls data from source, transform, push and checkpoint. Metrics collection and instrumentation APIs are exposed at this layer.
 2. The next layer uses distributed framework like Spark, to implement the portal operations at scale.
 3. Then the next layer deals with specific sources and sinks, checkpoints, and their metrics.
Development at this layer just need to implement single threaded workers, and does not need to be aware of any distributed operation.

There can be 2 checkpoint mode depending on the source. For Kafka, it's possible to perform distributed checkpoint for each topic-partition independently, and such mode is implemented in the 3rd layer.
For more general serial checkpoint like HDFS, s3, gcs, checkpoint is implemented via a portal stateTracker module at the 1st layer.

The entry point of blink is the PortalCaster, which only takes a single text configuration file (properties or xml) as input.
Highly customized portals can be build by extending the exposed APIs at various layers, and can be configured by dynamically loading the custom implementing classes.

To run a simple demo from command line (need to install sbt first):

`sbt test`

It should print traces to the console.