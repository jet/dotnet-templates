namespace Shipping.Watchdog.Lambda.Cdk

open Amazon.CDK
open Amazon.CDK.AWS.Lambda
open Propulsion.DynamoStore.Constructs
open System

type WatchdogLambdaStackProps
    (   // Source Sns FIFO Topic Arn, serviced by Propulsion.DynamoStore.Notifier
        notifierFifoTopicArn: string,
        
        // DynamoDB Index Table Name, written by Propulsion.DynamoStore.Indexer
        indexTableName: string,
        // DynamoDB Store Table Name
        storeTableName: string,

        // Path for published binaries for Watchdog.Lambda
        lambdaCodePath: string,
        
        // Description to apply to the Lambda
        lambdaDescription: string,
        // Handler invocation path
        lambdaHandler: string,

        // Lambda memory allocation - default 128 MB
        ?memorySize: int,
        // Lambda max batch size - default 10
        ?batchSize: int,
        // Lambda timeout - default 3m
        ?timeout: TimeSpan) =
    inherit StackProps()
    member val NotifierFifoTopicArn = notifierFifoTopicArn
    member val StoreTableName = storeTableName
    member val IndexTableName = indexTableName
    member val MemorySize = defaultArg memorySize 128
    member val BatchSize = defaultArg batchSize 10
    member val Timeout = defaultArg timeout (TimeSpan.FromMinutes 3)
    member val LambdaCodePath = lambdaCodePath
    member val LambdaDescription = lambdaDescription
    member val LambdaHandler = lambdaHandler
    member val LambdaArchitecture = Architecture.ARM_64
    member val LambdaRuntime = Runtime.DOTNET_6

type WatchdogLambdaStack(scope, id, props: WatchdogLambdaStackProps) as stack =
    inherit Stack(scope, id, props)

    let props: DynamoStoreReactorLambdaProps =
        {   updatesSource = UpdatesTopic props.NotifierFifoTopicArn
            regionName = stack.Region; storeTableName = props.StoreTableName; indexTableName = props.IndexTableName
            memorySize = props.MemorySize; batchSize = props.BatchSize; timeout = props.Timeout
            lambdaDescription = props.LambdaDescription
            codePath = props.LambdaCodePath; lambdaHandler = props.LambdaHandler
            lambdaArchitecture = props.LambdaArchitecture; lambdaRuntime = props.LambdaRuntime }
    let _ = DynamoStoreReactorLambda(stack, "Watchdog", props = props)
