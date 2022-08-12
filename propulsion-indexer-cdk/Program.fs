module IndexerCdkTemplate.Program

open Amazon.CDK

let streamNameArg = "dev/streamArn"
let indexTableNameArg = "dev/indexTableName"
// See logic in .fsproj file, which copies content from `Propulsion.DynamoStore.Lambda`'s tools/ folder to this well known location
let lambdaCodePath = "obj/pub/net6.0/linux-arm64/"

[<EntryPoint>]
let main _ =
    let app = App(null)

    // TODO probably replace with Logic to look it up in SSM Parameter Store
    let storeStreamArn, indexTableName =
        match app.Node.TryGetContext streamNameArg, app.Node.TryGetContext indexTableNameArg with
        | :? string as sa, (:? string as tn) when sa <> null && tn <> null -> sa, tn
        | _ -> failwith $"Please supply DynamoDB Streams ARN and DynamoDB Index Table Name via -c {streamNameArg}= and -c {indexTableNameArg}= respectively"
    let codePath =
        match app.Node.TryGetContext "code" with
        | :? string as path -> path
        | _ -> lambdaCodePath

    let _mainIndexer = IndexerStack(app, "MainIndexer", IndexerStackProps(storeStreamArn, indexTableName, codePath))
    // TOCONSIDER - if you have >1 table for your application, instantiate additional IndexerStacks here 

    app.Synth() |> ignore
    0