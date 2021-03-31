namespace global

module EnvVar =

    let tryGet varName : string option = System.Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    let forMetrics () =
        Serilog.Log.ForContext("isMetric", true)

module Equinox =

    let createDecider stream =
        Equinox.Decider(Log.forMetrics (), stream, maxAttempts = 3)

[<AutoOpen>]
module DiscoveryExtensions =

    let (|AccountEndpoint|) connectionString =
        match System.Data.Common.DbConnectionStringBuilder(ConnectionString = connectionString).TryGetValue "AccountEndpoint" with
        | true, (:? string as s) when not (System.String.IsNullOrEmpty s) -> s
        | _ -> invalidOp "Connection string does not contain an \"AccountEndpoint\""

    type Equinox.CosmosStore.Discovery with

        member x.Endpoint : System.Uri = x |> function
            | Equinox.CosmosStore.Discovery.AccountUriAndKey (u, _k) -> u
            | Equinox.CosmosStore.Discovery.ConnectionString (AccountEndpoint e) -> System.Uri e
