namespace global

module EnvVar =

    let tryGet varName : string option = System.Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    let forMetrics () =
        Serilog.Log.ForContext("isMetric", true)

module Equinox =

    let createDecider stream =
        Equinox.Decider(Log.forMetrics (), stream, maxAttempts = 3)
