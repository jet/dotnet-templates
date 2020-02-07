//#if kafka
# Propulsion EventStore -> Kafka Projector
//#else
# Propulsion EventStore Projector (without Kafka emission)
//#endif

This project was generated using:
//#if kafka

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proAllProjector -k # -k => include Kafka projection logic
//#else

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -k to add Kafka Projection logic
    dotnet new proAllProjector # use --help to see options
//#endif

## Usage instructions

0. establish connection strings etc. for the checkpoint store in CosmosDB per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use -c

        dotnet tool install -g Equinox.Tool # only needed once
        eqx init -ru 400 cosmos

    (Yes, someone should do a PR to store the checkpoints in EventStore itself; this is extracted from working code, which can assume there's always a CosmosDB around)

1. To run an instance of the Projector from EventStore

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them after the `cosmos` argument token)

        $env:EQUINOX_ES_USERNAME="admin" # or use -u
        $env:EQUINOX_ES_PASSWORD="changeit" # or use -p
        $env:EQUINOX_ES_HOST="localhost" # or use -g

//#if kafka
        $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `es` specifies the source (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos` specifies the checkpoint store (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        dotnet run -- -g default es cosmos kafka -t topic0
//#else
        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `es` specifies the source (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos` specifies the checkpoint store (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default es cosmos
//#endif

        # NB running more than one projector will cause them to duel, and is hence not advised

2. To create a Consumer, use `dotnet new proConsumer` (see README therein for details)