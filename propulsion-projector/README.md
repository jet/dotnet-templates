//#if kafka
# Propulsion CosmosDb -> Kafka Projector
//#else
# Propulsion CosmosDb Projector (without Kafka emission)
//#endif

This project was generated using:
//#if kafka

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proProjector -k # -k => include Kafka projection logic
//#else

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -k to add Kafka Projection logic
    dotnet new proProjector # use --help to see options
//#endif

## Usage instructions

0. establish connection strings etc. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_COLLECTION="equinox-test" # or use - c

1. Use the `eqx` tool to initialize and then run some transactions in a CosmosDb collection

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # generate a cosmos collection to store events in
        eqx init -ru 1000 cosmos

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-t saveforlater` SaveForLater test produces uniform size events to project
        # `-C -f 200` constrains current writers to 100 and applies caching so RU consumption is constrained such that an allocation of 1000 is sufficient
        eqx run -t saveforlater -C -f 100 cosmos 
//#if kafka

2. To run an instance of the Projector:

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        $env:EQUINOX_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

        # `default` defines the Projector Group identity - each id has separated state in the aux collection (aka LeaseId)
        # `-md 1000` sets the change feed maximum document limit to 1000
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        # cosmos specifies the source (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- default -md 1000 -t topic0 cosmos

        # (assuming you've scaled up enough to have >1 range, you can run a second instance in a second console with the same arguments)

3. To create a Consumer, use `dotnet new proConsumer`
//#else

2. To run an instance of the Projector:

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # `default` defines the Projector Group identity - each id has separated state in the aux collection (aka LeaseId)
        # `-m 1000` sets the max batch size to 1000
        # cosmos specifies the source (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- default -m 1000 cosmos

        # NB (assuming you've scaled up enough to have >1 range, you can run a second instance in a second console with the same arguments)
//#endif