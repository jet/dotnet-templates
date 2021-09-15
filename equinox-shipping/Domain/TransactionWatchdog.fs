/// Handles requirement to infer when a transaction is 'stuck'
/// Note we don't want to couple to the state in a deep manner; thus we track:
/// a) when the request intent is established (aka when *Requested is logged)
/// b) when a transaction reports that it has Completed
module Shipping.Domain.TransactionWatchdog

module Events =

    type Categorization =
        | NonTerminal of System.DateTimeOffset
        | Terminal
    let createCategorizationCodec isTerminalEvent =
        let tryDecode (encoded : FsCodec.ITimelineEvent<byte[]>) =
            Some (if isTerminalEvent encoded then Terminal else NonTerminal encoded.Timestamp)
        let encode _ = failwith "Not Implemented"
        let mapCausation _ = failwith "Not Implemented"
        FsCodec.Codec.Create<Categorization, _, obj>(encode, tryDecode, mapCausation)

module Fold =

    type State = Initial | Active of startTime: System.DateTimeOffset | Completed
    let initial = Initial
    let evolve state = function
        | Events.NonTerminal startTime->
            if state = Initial then Active startTime
            else state
        | Events.Terminal ->
            Completed
    let fold : State -> Events.Categorization seq -> State = Seq.fold evolve

type Status = Complete | Active | Stuck
let toStatus cutoffTime = function
    | Fold.Initial -> failwith "Expected at least one valid event"
    | Fold.Active startTime when startTime < cutoffTime -> Stuck
    | Fold.Active _ -> Active
    | Fold.Completed -> Complete

let fold : Events.Categorization seq -> Fold.State =
    Fold.fold Fold.initial

let (|TransactionStatus|) (codec : #FsCodec.IEventCodec<_, _, _>) events : Fold.State =
    events
    |> Seq.choose codec.TryDecode
    |> fold

module Finalization =

    let private codec = Events.createCategorizationCodec FinalizationTransaction.Reactions.isTerminalEvent
    let (|MatchStatus|_|) = function
        | FinalizationTransaction.StreamName transId, TransactionStatus codec status -> Some (transId, status)
        | _ -> None
