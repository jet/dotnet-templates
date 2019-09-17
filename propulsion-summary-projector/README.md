# Propulsion CosmosDb -> Kafka Summary Projector

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new summaryProjector

## Usage instructions

0. establish connection strings etc. for the container from which the summaries will be generated per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use - c

1a. Use the `eqx` tool to initialize and then run some transactions in a CosmosDb container

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # generate a cosmos container to store events in
        eqx init -ru 1000 cosmos

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-t todo` Todo test produces items the sample Aggregate logic knows how to summarize
        # `-C -f 200` constrains current writers to 100 and applies caching so RU consumption is constrained such that an allocation of 1000 is sufficient
        eqx run -t todo -C -f 100 cosmos 

1b. To run an instance of the Projector from CosmosDb

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

        # `default` defines the Projector Group identity - each id has separated state in the aux container (aka LeaseId)
        # `cosmos` specifies the source (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `-m 1000` sets the change feed maximum document limit to 1000
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        dotnet run -- default cosmos -m 1000 kafka -t topic0

        # (assuming you've scaled up enough to have >1 range, you can run a second instance in a second console with the same arguments)

2. To run an instance of the Projector from EventStore

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them after the `cosmos` argument token)

        $env:EQUINOX_ES_USERNAME="admin" # or use -u
        $env:EQUINOX_ES_PASSWORD="changeit" # or use -p
        $env:EQUINOX_ES_HOST="localhost" # or use -g

        $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

        # `default` defines the Projector Group identity - each id has separated state in the aux container (aka LeaseId)
        # `es` specifies the source (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos` specifies the destination and the checkpoint store (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        dotnet run -- default es cosmos kafka -t topic0

        # NB running more than one projector will cause them to duel, and is hence not advised

3. To create a Consumer, use `dotnet new summaryConsumer` (see README therein for details)