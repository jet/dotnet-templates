# Propulsion Kafka Tracking Consumer

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new trackingConsumer

## Usage instructions

0. Create and run an instance of the Projector in the source Container (see README in `dotnet new proProjector` for details)
   _NB none of the templates presently produce data in this format_

1. establish connection strings for the target container into which the summaries will synced. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use - c

2. To run an instance of the Consumer:

        $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b
        $env:PROPULSION_KAFKA_TOPIC="topic0" # or use -t
        $env:PROPULSION_KAFKA_GROUP="group0" # or use -g

        # `-t topic0` identifies the Kafka topic from which the consumers should read
        # `-g group0` identifies the Kafka consumer group among which the consumption is to be spread
        dotnet run -- -t topic0 -g group0 cosmos

        # (you can run as many instances as there are partitions configured for the topic on the broker)