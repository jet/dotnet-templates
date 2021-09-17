namespace TodoBackendTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module Equinox =

    /// Tag log entries so we can filter them out if logging to the console
    let log = Serilog.Log.ForContext("isMetric", true)
    let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value
