namespace DynamoStoreCdkTemplate

open Amazon.CDK
open Propulsion.DynamoStore.Constructs
open System

type NotifierStackProps
    (   // DynamoDB Streams Source ARN (for Index Table)
        indexStreamArn: string,

        // Target SNS FIFO Topic Arn (Default: Create fresh topic)
        updatesTopicArn: string option,

        // Path for published binaries for Propulsion.DynamoStore.Notifier
        lambdaCodePath: string,

        // Lambda memory allocation - default 128 MB
        ?memorySize: int,
        // Lambda max batch size - default 10
        ?batchSize: int,
        // Lambda max batch size - default 10s
        ?timeout: TimeSpan) =
    inherit StackProps()
    member val IndexStreamArn = indexStreamArn
    member val UpdatesTopicArn = updatesTopicArn
    member val MemorySize = defaultArg memorySize 128
    member val BatchSize = defaultArg batchSize 10
    member val Timeout = defaultArg timeout (TimeSpan.FromSeconds 10)
    member val LambdaCodePath = lambdaCodePath

type NotifierStack(scope, id, props: NotifierStackProps) as stack =
    inherit Stack(scope, id, props)

    let props: DynamoStoreNotifierLambdaProps =
        {   indexStreamArn = props.IndexStreamArn
            updatesTarget = match props.UpdatesTopicArn with Some ta -> UpdatesTarget.ExistingTopic ta | None -> UpdatesTarget.Default 
            memorySize = props.MemorySize; batchSize = props.BatchSize; timeout = props.Timeout
            codePath = props.LambdaCodePath }
    let _ = DynamoStoreNotifierLambda(stack, "Notifier", props = props)
