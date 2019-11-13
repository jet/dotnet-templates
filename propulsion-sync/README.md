# Propulsion Sync template

This project was generated using:

//#if marveleqx

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new proSync -m # -m - include Marvel V0 import logic

//#else

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -m to include Marvel V0 import logic
    dotnet new proSync # use --help to see options

//#endif

## Usage instructions

0. establish connection strings etc. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use - c

1. Use the `eqx` tool to initialize a CosmosDb container

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # generate a cosmos container to store events in
        eqx init -ru 1000 cosmos

2. We'll be operating a ChangeFeedProcessor, so use `eqx initaux` to make a `-aux` container (unless there already is one)

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # default name is "($EQUINOX_COSMOS_CONTAINER)-aux"
        eqx initaux -ru 400 cosmos

3. To run an instance of the Sync tool feeding into CosmosDb:

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `-g defaultSync` defines the Projector Group identity ('LeaseId') - each id has separated state in the aux container
        # `cosmos -s connection -d database -c container` specifies the input datasource
        # `-m 1000` sets the change feed item count limit to 1000
        # second `cosmos` specifies the destination (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)

        $env:EQUINOX_COSMOS_CONNECTION_SOURCE="AccountEndpoint=https://....;AccountKey=....=;" # or use -s # or defaults to EQUINOX_COSMOS_CONNECTION
        $env:EQUINOX_COSMOS_DATABASE_SOURCE="input-database" # or use -d # or defaults to EQUINOX_COSMOS_DATABASE
        $env:EQUINOX_COSMOS_CONTAINER_SOURCE="input-container" # or use -c # NB DOES NOT HAVE A DEFAULT VALUE

        dotnet run -- -g defaultSync `
            cosmos -m 1000 -s $env:EQUINOX_COSMOS_CONNECTION_SOURCE -d $env:EQUINOX_COSMOS_DATABASE_SOURCE -c $env:EQUINOX_COSMOS_CONTAINER_SOURCE `
            cosmos # Can add overrides for destination here

4. To run an instance of the Sync tool feeding into EventStore:

        # (this is in addition to details in step 3 above)
        # `es` specifies the destination (if you have specified 3x EQUINOX_ES_* environment vars, no arguments are needed)
        # `cosmos -s connection -d database -c container` specifies the input datasource

        $env:EQUINOX_ES_USERNAME="admin" # or use -u
        $env:EQUINOX_ES_PASSWORD="changeit" # or use -p
        $env:EQUINOX_ES_HOST="localhost" # or use -g

        dotnet run -- -g defaultSync `
            cosmos -s $env:EQUINOX_COSMOS_CONNECTION_SOURCE -d $env:EQUINOX_COSMOS_DATABASE_SOURCE -c $env:EQUINOX_COSMOS_CONTAINER_SOURCE `
            es # Can add overrides for destination here