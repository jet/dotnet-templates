# Equinox Testbed Template

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store

//#if eventStore
    # use -m=false to remove Equinox.MemoryStore support
    # use -c to add Equinox.Cosmos support
    dotnet new eqxtestbed -e # use --help to see options
//#elif cosmos
    # use -m=false to remove Equinox.MemoryStore support
    # use -m to add Equinox.MemoryStore support
    # use -e to add Equinox.EventStore support
    dotnet new eqxtestbed -c # use --help to see options
//#else
    # use -m=false to remove Equinox.MemoryStore support
    # use -e to add Equinox.EventStore support
    # use -c to add Equinox.Cosmos support
    dotnet new eqxtestbed  # use --help to see options
//#endif

## Usage instructions

0. provision stores, establish connection strings etc. per [the QuickStart in the Equinox README](https://github.com/jet/equinox#quickstart)

1. read the [Equinox README section on Benchmarks](https://github.com/jet/equinox#benchmarks)

2. running against in-Memory Store (can be removed with `-m=false` if not invoked with `-e` or `-c`)

    It can be useful to baseline the intrinsic cost of your model's processing, including serialization/deserialization and the baseline costs of the Equinox Decision processing in the `Equinox` and `Equinox.Codec` packages without going out of process. This uses `Equinox.MemoryStore`, which uses a `ConcurrentDictionary` to store the streams. Tens of thousands of RPS are normally attainable without any issues

       # run against in-memory store 10000rps, for 1 minute 
       dotnet run -- run -d 1 -f 10000 memory

2. running against EventStore (iff run with `-e`)

    The commandline `dotnet run -p Testbed -- run es -help` shows the various connection options you can apply when using EventStore. Running without any arguments will run against a local EventStore. See the Equinox readme for Mac installation instructions. For most configs, you should be able to run 2000rps against a local EventStore on an SSD without a network in the mix.

        cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows # OR brew install eventstore-oss etc

        # with caching against EventStore
        dotnet run -- run -C -f 2000 es

3. running against CosmosDb using `Equinox.Cosmos` (iff run with `-c`)

    Provisioning a CosmosDb is detailed in the Equinox README. TL;DR

        # establish CosmosDb settings
        $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;" # or use -s
        $env:EQUINOX_COSMOS_DATABASE="equinox-test" # or use -d
        $env:EQUINOX_COSMOS_CONTAINER="equinox-test" # or use - c

        dotnet tool install -g Equinox.Tool # only needed once
        eqx init -ru 1000 cosmos

    The commandline `dotnet run -p Testbed -- run cosmos -help` shows the various connection options available when using CosmosDb. Running without any arguments will use the environment variables as above. While it's possible to run against the CosmosDb simulator, there's not much to be learned over what one can learn from using `Equinox.MemoryStore`. In general, if the transactions are small and you don't saturate the network bandwidth, you should be able to achieve 100s of rps even if you're not in the same Region. Running in the same data center as the CosmosDb instance, 1000s of rps should be attainable.

        # Run with caching and unfolds against CosmosDb, 100 rps
        dotnet run -- run -C - U -d 2 -f 100 cosmos