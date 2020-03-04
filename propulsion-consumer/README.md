# Propulsion Kafka Consumer

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proConsumer

## Usage instructions

0. Run an instance of the Projector (`dotnet new proProjector`)

1. To run an instance of the Consumer:

        $env:PROPULSION_KAFKA_BROKER="instance.kafka.example.com:9092" # or use -b
        $env:PROPULSION_KAFKA_TOPIC="topic0" # or use -t
        $env:PROPULSION_KAFKA_GROUP="group0" # or use -g

        # `-t topic0` identifies the Kafka topic from which the consumers should read
        # `-g group0` identifies the Kafka consumer group among which the consumption is to be spread
        dotnet run -- -t topic0 -g group0

        # (you can run as many instances as there are partitions configured for the topic on the broker)