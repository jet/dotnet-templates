[<AutoOpen>]
module FeedSourceTemplate.Domain.Infrastructure

module Equinox =

    /// Tag log entries so we can filter them out if logging to the console
    let log = Serilog.Log.ForContext("isMetric", true)
    let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)
