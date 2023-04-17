# distributed-system-part1-Jennytang1224

## **Producer**

You will implement a `Producer` API that may be used by an application running on any host  that publishes messages to a broker. At minimum, your `Producer` will allow the application to do the following:

1. Connect to a `Broker`
2. Send data to the `Broker` by providing a `byte[]` containing the data and a `String` containing the topic.

## Consumer

You will implement a `Consumer` API that may be used by an application running on any host that consumes messages from a broker. At minimum, your `Consumer` will allow the application to do the following:

1. Connect to a `Broker`
2. Retrieve data from the `Broker` using a pull-based approach by specifying a topic of interest and a starting position in the message stream

## Broker

The `Broker` will accept an unlimited number of connection requests from producers and consumers. The basic `Broker` implementation* will maintain a thread-safe, in-memory data structure that stores all messages. The basic `Broker` will be stateless with respect to the `Consumer` hosts.

three Producers, three Consumers, and one Broker.

