# Equinox Projector Template

This project was generated using:

    ```powershell
    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
//#if !kafka
    # add -k to add Kafka Projection logic and consumer
    dotnet new eqxprojector # use --help to see options such as 
//#else
    dotnet new eqxprojector -k # -k => include projection logic and consumer
//#endif
    ```

To provide sample data to project:
    0. establish connection strings etc. per https://github.com/jet/equinox README

        ```powershell
        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
        $env:EQUINOX_COSMOS_DATABASE="equinox-test"
        $env:EQUINOX_COSMOS_COLLECTION="equinox-test"
        ```

    1. Use the eqx tool to initialize and then run some transactions in a CosmosDb collection

        ```powershell
        dotnet tool install Equinox.Tool
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # generate a cosmos collection to store events in
        eqx init -ru 1000 cosmos
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-t saveforlater` SaveForLater test produces uniform size events to project
        # `-C -f 200` constrains current writers to 100 and applies caching so RU consumption is constrained such that an allocation of 1000 is sufficient
        eqx run -t saveforlater -C -f 100 cosmos 
        ```
//#if !kafka

    2. To run the Projector:

        ```powershell
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-b 1000` sets the max batch size to 1000
        # `-n epoch0` defines the Projector instance id - each id has separated state in the aux collection
        dotnet run -p Projector -- -n epoch0 -b 1000
        # NB (assuming you've scaled up enough to have >1 range, you can run a second instance in a second console with the same arguments)
        ```
//#else

    2. To run the Projector:

        ```powershell
        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
        $env:EQUINOX_COSMOS_DATABASE="equinox-test"
        $env:EQUINOX_COSMOS_COLLECTION="equinox-test"
        $env:EQUINOX_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b
        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-m 1000` sets the max batch size to 1000
        # `epoch0` defines the Projector instance id - each id has separated state in the aux collection
        # `-t topic0` identifies the Kafka topic which the Projector is to write to
        dotnet run -p Projector -- epoch0 -m 1000 -t topic0 cosmos
        # NB (assuming you've scaled up enough to have >1 range, you can run a second instance in a second console with the same arguments)
        ```

    3. To run the Consumer:

        ```powershell
        $env:EQUINOX_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b
        $env:EQUINOX_KAFKA_TOPIC="topic0" # or use -t
        $env:EQUINOX_KAFKA_GROUP="group0" # or use -g
        # `-t topic0` identifies the Kafka topic which the Projector is to write to
        dotnet run -p Consumer -- -t topic0 -g group0
        ```
//#endif