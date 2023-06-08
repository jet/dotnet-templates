namespace DynamoStoreCdkTemplate

open Amazon.CDK
open Propulsion.DynamoStore.Constructs
open System

type IndexerStackProps
    (   // DynamoDB Streams Source ARN (for Store Table)
        storeStreamArn: string,

        // DynamoDB Index Table Name
        indexTableName: string,

        // Path for published binaries for Propulsion.DynamoStore.Indexer
        lambdaCodePath: string,

        // Lambda memory allocation - default 128 MB
        ?memorySize: int,
        // Lambda max batch size - default 1000
        ?batchSize: int,
        // Lambda max batch size - default 180s
        ?timeout: TimeSpan) =
    inherit StackProps()
    member val StoreStreamArn = storeStreamArn
    member val IndexTableName = indexTableName
    member val MemorySize = defaultArg memorySize 128
    member val BatchSize = defaultArg batchSize 1000
    member val Timeout = defaultArg timeout (TimeSpan.FromSeconds 180)
    member val LambdaCodePath = lambdaCodePath

type IndexerStack(scope, id, props: IndexerStackProps) as stack =
    inherit Stack(scope, id, props)

    let props: DynamoStoreIndexerLambdaProps =
        {   storeStreamArn = props.StoreStreamArn
            regionName = stack.Region; indexTableName = props.IndexTableName
            memorySize = props.MemorySize; batchSize = props.BatchSize; timeout = props.Timeout
            codePath = props.LambdaCodePath }
    let _ = DynamoStoreIndexerLambda(stack, "Indexer", props = props)
