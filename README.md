# distributed-system-part1-Jennytang1224

For this project, I implemented a distributed publish/subscribe system similar to the original Kafka design.

## **Producer**

Implemented a `Producer` API that may be used by an application running on any host  that publishes messages to a broker. The `Producer` allows the application to do the following:

1. Connect to a `Broker`
2. Send data to the `Broker` by providing a `byte[]` containing the data and a `String` containing the topic.

## Consumer

Implemented a `Consumer` API that may be used by an application running on any host that consumes messages from a broker. The `Consumer` will allow the application to do the following:

1. Connect to a `Broker`
2. Retrieve data from the `Broker` using a pull-based approach by specifying a topic of interest and a starting position in the message stream

## Broker

The `Broker` accepts an unlimited number of connection requests from producers and consumers. The basic `Broker` implementation* will maintain a thread-safe, in-memory data structure that stores all messages. The basic `Broker` will be stateless with respect to the `Consumer` hosts.

In total three Producers, three Consumers, and one Broker.

## Partitioning

Built multiple instances of the `Broker` running on separate hosts. Each topic may have multiple partitions, and each partition may be handled by different  `Broker`. When a new message is posted it specifies both the topic and a key. Like in the real Kafka implementation, the key will be hashed to determine which `Broker` is managing the partition for that <key, topic>. I also designed the mechanism for directing a request to the appropriate `Broker` by implementing a custom load balancer that is essentially just another service that accepts a request containing a key and returns the host information of the `Broker` that manages that partition.


