# Propulsion CosmosDb ChangeFeedProcessor Reactor

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proCosmosReactor # use --help to see options

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

3. To run an instance of the Reactor from a CosmosDb ChangeFeed

        # `-g default` defines the `processorName` - each processor group name has separated state in the leases store
        # `-c MyContainer` specifies the source Container to monitor (if you have specified EQUINOX_COSMOS_* environment
        #   vars, no connection/database/container arguments are needed.)
        # For this template, this same container is also used to wire up the Connection used for the Reactions processing.
        # See the `proReactor` template for a more complex variant that lets you specify them separately.
        dotnet run -- -g default cosmos -c MyContainer
