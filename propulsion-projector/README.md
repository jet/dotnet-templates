//#if cosmos
//#if   kafka // cosmos && kafka
# Propulsion CosmosDB -> Kafka Projector

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proProjector -k # -k => include Kafka projection logic
//#else // cosmos && !kafka
# Propulsion CosmosDB Projector (without Kafka emission)

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -k to add Kafka Projection logic
    dotnet new proProjector # use --help to see options
//#endif // cosmos && !kafka
//#endif // cosmos
//#if esdb
//#if   kafka // esdb && kafka
# Propulsion EventStoreDB -> Kafka Projector

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proProjector -s eventStore -k # -k => include Kafka projection logic
//#else // esdb && !kafka
# Propulsion EventStoreDB Projector (without Kafka emission)

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -k to add Kafka Projection logic
    dotnet new proProjector -s eventStore # use --help to see options
//#endif // esdb && !kafka
//#endif // esdb

## Usage instructions

0. establish connection strings etc. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use -c

//#if cosmos
1a. Use the `eqx` tool to initialize and then run some transactions in a CosmosDB container

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # generate a cosmos container to store events in
        eqx init -ru 1000 cosmos

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-t saveforlater` SaveForLater test produces uniform size events to project
        # `-C -f 200` constrains current writers to 100 and applies caching so RU consumption is constrained such that an allocation of 1000 is sufficient
        eqx run -t saveforlater -C -f 100 cosmos

1b. We'll be operating a ChangeFeedProcessor, so use `propulsion init` to make a `-aux` container (unless there already is one)

        dotnet tool install -g Propulsion.Tool
        
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # default name is "($EQUINOX_COSMOS_CONTAINER)-aux"
        propulsion init -ru 400 cosmos
//#endif // cosmos
//#if esdb
1. Use the `eqx` tool to initialize the checkpoints container

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # generate a cosmos container to store checkpoints in
        eqx init -ru 400 cosmos
//#endif // esdb
         
2. To run an instance of the Projector:

//#if esdb
        $env:EQUINOX_ES_HOST="localhost" # or use -h after the `es` token in the arguments
        $env:EQUINOX_ES_USERNAME="admin" # or use -u after the `es` token in the arguments
        $env:EQUINOX_ES_PASSWORD="changeit" # or use -p after the `es` token in the arguments

//#endif // esdb
//#if kafka
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        $env:PROPULSION_KAFKA_BROKER="instance.kafka.example.com:9092" # or use -b

//#if   cosmos // kafka && cosmos
        # `-g default` defines the Projector Group identity - each id has separated state in the aux container (aka LeaseId)
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        # cosmos specifies the source (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `-md 1000` sets the change feed maximum document limit to 1000
        dotnet run -- -g default -t topic0 cosmos -md 1000

        # (assuming you've scaled up enough to have >1 physical partition range, you can run a second instance [in a second console] with the same arguments)
//#endif // kafka && cosmos
//#if   esdb
        # `-g default` defines the Projector Group identity - each id has a separate checkpoint in the EQUINOX_COSMOS_CONTAINER
        # `-t topic0` identifies the Kafka topic to which the Projector should write
        # es specifies the source details (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # cosmos specifies the checkpoint store details (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        dotnet run -- -g default -t topic0 es cosmos
//#endif // kafka && esdb

3. To create a Consumer, use `dotnet new proConsumer` or `dotnet new proReactor --source kafkaEventSpans`
//#else // !kafka
//#if   cosmos
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # `-g default` defines the Projector Group identity - each id has separated state in the aux container (aka LeaseId)
        # cosmos specifies the source (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `-md 1000` sets the max batch size to 1000
       dotnet run -- -g default cosmos -md 1000 

        # NB (assuming you've scaled up enough to have >1 physical partition range, you can run a second instance in a second console with the same arguments)
//#endif // !kafka && cosmos
//#if   esdb
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)

        # `-g default` defines the Projector Group identity - each id has a separate checkpoint in the EQUINOX_COSMOS_CONTAINER
        # es specifies the source details (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # cosmos specifies the checkpoint store details (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
       dotnet run -- -g default es cosmos
//#endif // !kafka && esdb       
//#endif // !kafka