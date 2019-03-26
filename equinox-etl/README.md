# Equinox.Cosmos ETL template

This project was generated using:

//#if marveleqx
    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new eqxetl -m # -m - include Marvel V0 import logic
//#else
    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    # add -m to include Marvel V0 import logic
    dotnet new eqxetl # use --help to see options
//#endif

## Usage instructions

0. establish connection strings etc. per https://github.com/jet/equinox README

        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_COLLECTION="equinox-test" # or use - c

1. Use the `eqx` tool to initialize a CosmosDb collection

        dotnet tool install -g Equinox.Tool # only needed once

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # generate a cosmos collection to store events in
        eqx init -ru 1000 cosmos

2. We'll be operating a changefeed, so use `eqx initaux` to make a `-aux` collection (unless there already is one)

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # default name is "($EQUINOX_COSMOS_COLLECTION)-aux"
        eqx initaux -ru 400 cosmos

3. To run an instance of the Etl tool

        # (either add environment variables as per step 0 or use -s/-d/-c to specify them)
        # `defaultEtl` defines the Projector Group identity - each id has separated state in the aux collection (aka LeaseId)
        # `-m 1000` sets the max batch size to 1000
        # `cosmos` specifies the destination (if you have specified 3x EQUINOX_COSMOS_* environment vars, no arguments are needed)
        # `source -s connection -d database -c collection` specifies the input datasource

        $env:EQUINOX_COSMOS_CONNECTION_SOURCE="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE_SOURCE="input-database" # or use -d
        $env:EQUINOX_COSMOS_COLLECTION_SOURCE="input_collection" # or use - c

        dotnet run -p Etl -- defaultEtl `
            source -s $env:EQUINOX_COSMOS_CONNECTION_SOURCE -d $env:EQUINOX_COSMOS_DATABASE_SOURCE -c $env:EQUINOX_COSMOS_COLLECTION_SOURCE `
            cosmos # Can add overrides for destination here