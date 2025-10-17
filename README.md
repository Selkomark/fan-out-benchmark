# Fan-out Benchmark

When building scalable backend systems, lightning-fast and reliable inter-service communication is key to maintaining high performance. In my experience, Redis has excelled in the pub/sub role—but after learning about NATS, I wanted to see if Redis still holds the top spot for fan-out messaging, or if NATS is the new leader.

To answer this, I designed this project to address some core questions:

- How quickly can Redis distribute messages to all subscribers in a fan-out scenario?
- How efficiently does Redis handle high volumes of published messages?
- How do Redis and NATS perform under stress and high traffic?
- Which is better suited for horizontal scaling and fan-in/fan-out patterns: Redis or NATS?

# Tech Stack

- [Docker](https://www.docker.com/)
- [C++20](https://en.cppreference.com/w/cpp/20.html)
- [Redis](https://redis.io/)
- [NATS](https://nats.io/)

# How to Run the Benchmark

To get accurate results, you can fine-tune the test parameters—such as number of subscribers, publishers, and duration—by editing your `.env` file:

```dotenv
NUM_PUBLISHERS=1
PUBLISH_DURATION_SECONDS=5
NUM_SUBSCRIBERS=3
```

Once you've set your configuration, simply run:

```
./scripts/run-docker-benchmark.sh
```

Enjoy benchmarking!
