module App.Args

open Argu
open System

let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"
let [<Literal>] VIEWS =                     "EQUINOX_COSMOS_VIEWS"

type Configuration(tryGet: string -> string option) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member x.CosmosConnection =             get CONNECTION
    member x.CosmosDatabase =               get DATABASE
    member x.CosmosContainer =              get CONTAINER
    member x.CosmosViews =                  get VIEWS

type [<NoEquality; NoComparison>] CosmosParameters =
    | [<AltCommandLine "-V"; Unique>]       Verbose
    | [<AltCommandLine "-s">]               Connection of string
    | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
    | [<AltCommandLine "-d">]               Database of string
    | [<AltCommandLine "-c">]               Container of string
    | [<AltCommandLine "-v">]               Views of string
    | [<AltCommandLine "-o">]               Timeout of float
    | [<AltCommandLine "-r">]               Retries of int
    | [<AltCommandLine "-rt">]              RetriesWaitTime of float
    interface IArgParserTemplate with
        member p.Usage = p |> function
            | Verbose ->                    "request Verbose Logging from Store. Default: off"
            | ConnectionMode _ ->           "override the connection mode. Default: Direct."
            | Connection _ ->               $"specify a connection string for a Cosmos account. (optional if environment variable ${CONNECTION} specified)"
            | Database _ ->                 $"specify a database name for store. (optional if environment variable ${DATABASE} specified)"
            | Container _ ->                $"specify a container name for store. (optional if environment variable ${CONTAINER} specified)"
            | Views _ ->                    $"specify a views Container name for Cosmos views. (optional if environment variable ${VIEWS} specified)"
            | Timeout _ ->                  "specify operation timeout in seconds. Default: 5."
            | Retries _ ->                  "specify operation retries. Default: 1."
            | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
and CosmosArguments(c: Configuration, p: ParseResults<CosmosParameters>) =
    let connection =                        p.GetResult(Connection, fun () -> c.CosmosConnection)
    let connector =
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let mode =                          p.TryGetResult ConnectionMode
        Equinox.CosmosStore.CosmosStoreConnector(Equinox.CosmosStore.Discovery.ConnectionString connection, timeout, retries, maxRetryWaitTime, ?mode = mode)
    member val Verbose =                    p.Contains Verbose
    member val Connection =                 connection
    member val Database =                   p.GetResult(Database,  fun () -> c.CosmosDatabase)
    member val Container =                  p.GetResult(Container, fun () -> c.CosmosContainer)
    member val private Views =              p.GetResult(Views,     fun () -> c.CosmosViews)
    member x.Connect() =                    connector.Connect(x.Database, x.Container, x.Views)

type [<NoEquality; NoComparison>] CosmosSourceParameters =
    | [<AltCommandLine "-V"; Unique>]       Verbose
    | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
    | [<AltCommandLine "-s">]               Connection of string
    | [<AltCommandLine "-d">]               Database of string
    | [<AltCommandLine "-c">]               Container of string
    | [<AltCommandLine "-v">]               Views of string
    | [<AltCommandLine "-o">]               Timeout of float
    | [<AltCommandLine "-r">]               Retries of int
    | [<AltCommandLine "-rt">]              RetriesWaitTime of float

    | [<AltCommandLine "-lcs"; Unique>]     LeaseContainerSuffix of string
    | [<AltCommandLine "-a"; Unique>]       LeaseContainer of string
    | [<AltCommandLine "-Z"; Unique>]       FromTail
    | [<AltCommandLine "-b"; Unique>]       MaxItems of int
    | [<AltCommandLine "-l"; Unique>]       LagFreqM of float
    interface IArgParserTemplate with
        member p.Usage = p |> function
            | Verbose ->                    "request Verbose Logging from ChangeFeedProcessor and Store. Default: off"
            | ConnectionMode _ ->           "override the connection mode. Default: Direct."
            | Connection _ ->               $"specify a connection string for a Cosmos account. (optional if environment variable {CONNECTION} specified)"
            | Database _ ->                 $"specify a database name for store. (optional if environment variable {DATABASE} specified)"
            | Container _ ->                $"specify a container name for store. (optional if environment variable {CONTAINER} specified)"
            | Views _ ->                    $"specify a container name for views container. (optional if environment variable {VIEWS} specified)"
            | Timeout _ ->                  "specify operation timeout in seconds. Default: 5."
            | Retries _ ->                  "specify operation retries. Default: 1."
            | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

            | LeaseContainerSuffix _ ->     "specify Container Name suffix for Leases container. Default: `-aux`."
            | LeaseContainer _ ->           "specify Container Name (in this [target] Database) for Leases container. Default: `<Container>` + `-aux`."
            | FromTail ->                   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
            | MaxItems _ ->                 "maximum item count to request from the feed. Default: unlimited."
            | LagFreqM _ ->                 "specify frequency (minutes) to dump lag stats. Default: 1"
and CosmosSourceArguments(c: Configuration, p: ParseResults<CosmosSourceParameters>) =
    let connection =                        p.GetResult(Connection, fun () -> c.CosmosConnection)
    let connector =
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let mode =                          p.TryGetResult ConnectionMode
        Equinox.CosmosStore.CosmosStoreConnector(Equinox.CosmosStore.Discovery.ConnectionString connection, timeout, retries, maxRetryWaitTime, ?mode = mode)
    let database =                          p.GetResult(Database,  fun () -> c.CosmosDatabase)
    let containerId =                       p.GetResult(Container, fun () -> c.CosmosContainer)
    let viewsContainerId =                  p.GetResult(Views,     fun () -> c.CosmosViews)

    let suffix =                            p.GetResult(LeaseContainerSuffix, "-aux")
    let leaseContainerId =                  p.GetResult(LeaseContainer, containerId + suffix)

    let fromTail =                          p.Contains FromTail
    let maxItems =                          p.TryGetResult MaxItems
    let tailSleepInterval =                 TimeSpan.FromMilliseconds 500.
    let lagFrequency =                      p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes

    member val IsLagFreqSpecified =         p.Contains LagFreqM
    member val Verbose =                    p.Contains Verbose
    member val Connection =                 connection
    member val Database =                   database
    member _.ConnectWithFeed(?lsc) =        connector.ConnectWithFeed(database, containerId, viewsContainerId, leaseContainerId, ?logSnapshotConfig = lsc)
    member _.ConnectWithFeedReadOnly(auxClient, auxDatabase, auxContainerId) =
                                            connector.ConnectWithFeedReadOnly(database, containerId, viewsContainerId, auxClient, auxDatabase, auxContainerId)
    member val MonitoringParams =           fromTail, maxItems, tailSleepInterval, lagFrequency
