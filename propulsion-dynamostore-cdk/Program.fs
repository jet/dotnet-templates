module DynamoStoreCdkTemplate.Program

// See logic in .fsproj file, which extracts content from `Propulsion.DynamoStore.Indexer` and `Notifier`'s
// from each respective NuGet package's `tools/` folder into this well known location
let lambdaCodePath lambdaName = $"obj/pub/%s{lambdaName}/net6.0/linux-arm64/"

(* Indexer-related *)
let streamNameArg = "streamArn"
let indexTableNameArg = "indexTableName"

(* Notifier-related *)
let indexStreamArg = "indexStreamArn"
let topicNameArg = "notifyTopicArn"

[<EntryPoint>]
let main _ =
    let app = Amazon.CDK.App(null)

    let getArg = app.Node.TryGetContext // TODO probably replace with Logic to look it up in SSM Parameter Store
    let storeStreamArn, indexTableName =
        match getArg streamNameArg, getArg indexTableNameArg with
        | :? string as sa, (:? string as tn) when sa <> null && tn <> null -> sa, tn
        | _ -> failwith $"Please supply DynamoDB Streams ARN and DynamoDB Index Table Name via -c %s{streamNameArg}= and -c %s{indexTableNameArg}= respectively"
    let indexerCodePath = match getArg "indexerCode" with :? string as path -> path | _ -> lambdaCodePath "Indexer"

    let _mainIndexer = IndexerStack(app, "MainIndexer", IndexerStackProps(storeStreamArn, indexTableName, indexerCodePath))
    // TOCONSIDER - if you have >1 table for your application, instantiate additional IndexerStacks here
    
    // NOTE the Notifier Lambda is an optional component, only of relevance when you're utilizing Lambda Reactors
    // (the more common arrangement is to have a long-running host application poll direct from the index using a DynamoStoreSource)
    let notifierCodePath = match getArg "notifierCode" with :? string as path -> path | _ -> lambdaCodePath "Notifier"
    let indexStreamArn, notifyTopicArn =
        match getArg indexStreamArg, getArg topicNameArg with
        | :? string as sa, null when sa <> null -> sa, None
        | :? string as sa, (:? string as tn) when sa <> null -> sa, Option.ofObj tn
        | _ -> failwith $"Please supply -c %s{indexStreamArg}=<DynamoDB Index Streams ARN> and (optionally) -c %s{topicNameArg}=<SNS FIFO Topic ARN>"
    let _mainNotifier = NotifierStack(app, "MainNotifier", NotifierStackProps(indexStreamArn, notifyTopicArn, notifierCodePath))

    app.Synth() |> ignore
    0
