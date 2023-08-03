## DynamoStoreCdk template

Given a pair of DynamoDB Tables (provisioned using the `eqx` tool; see below)
- Store table: holds application events
  - read/written via `Equinox.DynamoStore`
  - appends are streamed via DynamoDB Streams with a 24h retention period
- Index table: holds an index for the table (internally the index is simply an `Equinox.DynamoStore`)
  - written by `Propulsion.DynamoStore.Indexer`
  - read by `Propulsion.DynamoStore.DynamoStoreSource` in Reactor/Projector applications

This project uses the [AWS Cloud Development Kit (CDK)](https://docs.aws.amazon.com/cdk/v2/guide/home.html) to reliably manage configuration/deployment of:

1. The indexing logic in `Propulsion.DynamoStore.Indexer` (the NuGet package includes the published binaries internally)
2. Associated role and triggers that route from the Store Table's DynamoDB Stream to the Indexer Lambda and permissions to enable writing to the Index Table
   (the project references `Propulsion.DynamoStore.Constructs.DynamoStoreIndexerLambda`, and `Propulsion.DynamoStore.Indexer`)
3. Associated role and triggers that route from the Index Table's DynamoDB Stream to the Notifier Lambda, which publishes to an SNS Topic
   (the project references `Propulsion.DynamoStore.Constructs.DynamoStoreNotifierLambda`, and `Propulsion.DynamoStore.Notifier`)

## Prerequisites

1. A source DynamoDB Table, with DDB Streams configured

       eqx initaws -r 10 -w 10 -s new dynamo -t equinox-test

2. An index DynamoDB Table (with Notifier support)

       eqx initaws -r 5 -w 5 -s new dynamo -t equinox-test-index

   *OR (without Notifier support)*

       eqx initaws -r 5 -w 5 -s off dynamo -t equinox-test-index

3. AWS CDK Toolkit installed

       npm install -g aws-cdk

   See https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_install for more details/context

## To deploy

    # see below to look up DDB Streams ARNs for streamArn and indexStreamArn respectively
    cdk deploy --all \
        -c streamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013 \
        -c indexTableName=equinox-test-index \
        -c indexStreamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test-index/stream/2022-09-28T15:39:09.003

## To deploy local build of the Indexer Code (as opposed to extracting from the nuget package)

    dotnet publish ../../propulsion/src/Propulsion.DynamoStore.Indexer &&
    cdk deploy MainIndexer \
        -c indexerCode=../../propulsion/src/Propulsion.DynamoStore.Indexer/bin/Debug/net6.0/linux-arm64/publish \
        -c streamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013 \
        -c indexTableName=equinox-test-index

## To deploy local build of the Notifier Code (as opposed to extracting from the nuget package)

    dotnet publish ../propulsion/src/Propulsion.DynamoStore.Notifier &&
    cdk deploy MainNotifier \
        -c notifierCode=../propulsion/src/Propulsion.DynamoStore.Notifier/bin/Debug/net6.0/linux-arm64/publish \
        -c indexStreamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test-index/stream/2022-09-28T15:39:09.003 \
        -c notifyTopicArn=arn:aws:sns:us-east-1:111111111111:DynamoStore-not
    # NOTE: notifyTopicArn is optional - default is to create a fresh SNS topic 

## To determine/verify the Streams ARN for a given Table

    eqx stats dynamo -t equinox-test
    eqx stats dynamo -t equinox-test-index

## Useful commands

* `dotnet build`     check the CDK logic builds (no particular need to do this as `synth`/`deploy` triggers this implicitly)
* `cdk ls`           list all stacks in the app
* `cdk synth`        emits the synthesized CloudFormation template
* `cdk deploy`       deploy this stack to your default AWS account/region
* `cdk diff`         compare deployed stack with current state
* `cdk docs`         open CDK documentation
