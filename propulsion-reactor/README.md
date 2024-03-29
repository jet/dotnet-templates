//#if kafka
# Propulsion EventStore $all/CosmosDb ChangeFeedProcessor/DynamoStoreSource -> Kafka Projector
//#else
# Propulsion EventStore $all/CosmosDb ChangeFeedProcessor/DynamoStoreSource Projector (without Kafka emission)
//#endif

This project was generated using:
//#if kafka

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proReactor -k # -k => include Kafka projection logic
//#else

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -k to add Kafka Projection logic
    dotnet new proReactor # use --help to see options
//#endif

## Usage instructions

0. establish connection strings etc. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use -c

1. Use the `eqx` tool to initialize a CosmosDb container

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # generate a cosmos container to store events in
        eqx init -ru 400 cosmos

2. We'll be operating a ChangeFeedProcessor, so use `propulsion init` to make a `-aux` container (unless there already is one)

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # default name is "($EQUINOX_COSMOS_CONTAINER)-aux"
        propulsion init -ru 400 cosmos

    NOTE when projecting from EventStore, the current implementation stores the checkpoints within a CosmosStore or DynamoStore in order to remove feedback effects.

    (Yes, someone should do a PR to store the checkpoints in EventStore itself; this is extracted from working code, which can assume there's always a CosmosDB around)

3. To run an instance of the Projector from a CosmosDb ChangeFeed

//#if kafka
        $env:PROPULSION_KAFKA_BROKER="instance.kafka.example.com:9092" # or use -b

        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        # `-c $env:EQUINOX_COSMOS_CONTAINER ` specifies the source (if you have specified 2x EQUINOX_COSMOS_* environment vars, no connection/database arguments are needed, but the monitored (source) container must be specified explicitly)
        # the second `cosmos` specifies the target store for the reactions (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default kafka -t topic0 cosmos -c $env:EQUINOX_COSMOS_CONTAINER cosmos
//#else
        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `-c $env:EQUINOX_COSMOS_CONTAINER ` specifies the source (if you have specified EQUINOX_COSMOS_* environment vars, no connection/database arguments are needed, but the monitored (source) container must be specified explicitly)
        # `cosmos` specifies the target store for the reactions (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default cosmos -c $env:EQUINOX_COSMOS_CONTAINER  cosmos
//#endif

4. To run an instance of the Projector from EventStore

        # (either add environment variables like this, or use -h/-u/-p to specify them after the `es` argument token)

        $env:EQUINOX_ES_HOST="localhost" # or use -h
        $env:EQUINOX_ES_USERNAME="admin" # or use -u
        $env:EQUINOX_ES_PASSWORD="changeit" # or use -p

//#if kafka
        $env:PROPULSION_KAFKA_BROKER="instance.kafka.example.com:9092" # or use -b

        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        # `es` specifies the source (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos` specifies the checkpoint store (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default kafka -t topic0 es cosmos
//#else
        # `-g default` defines the Projector Group identity - each id has separated state in the checkpoints store (`Sync-default` in the cited `cosmos` store)
        # `es` specifies the source (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos` specifies the checkpoint store (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default es cosmos
//#endif

        # NB running more than one projector will cause them to duel, and is hence not advised

5. To create a Consumer, use `dotnet new proConsumer` (see README therein for details)
