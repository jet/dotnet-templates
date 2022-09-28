## IndexerCdkTemplate

Given a pair of DynamoDB Tables (provisioned using the `eqx` tool; see below)
- Store table: holds application events
  - read/written via `Equinox.DynamoStore`
  - appends are streamed via DynamoDB Streams with a 24h retention period
- Index table: holds an index for the table
  - written by `Propulsion.DynamoStore.Indexer`
  - read by `Propulsion.DynamoStore.DynamoStoreSource` in Reactor/Projector applications
  
  (internally the index is simply an `Equinox.DynamoStore`)

This project Uses the [AWS Cloud Development Kit (CDK)](https://docs.aws.amazon.com/cdk/v2/guide/home.html) to reliably manage configuration/deployment of:

1. The indexing logic in `Propulsion.DynamoStore.Indexer` (the NuGet package includes the published binaries internally)
2. Associated role and triggers that route from the DynamoDB Stream to the Lambda (the project references `Propulsion.DynamoStore.Constructs`
   which has the relevant configuration logic for `Propulsion.DynamoStore.Indexer`)

## Prerequisites

1. A source DynamoDB Table, with DDB Streams configured

       eqx initaws -r 10 -w 10 dynamo -t equinox-test

2. An index DynamoDB Table

       eqx initaws -r 5 -w 5 -s off dynamo -t equinox-test-index

3. AWS CDK Toolkit installed

       npm install -g aws-cdk

   See https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_install for more details/context

## To deploy

    cdk deploy -c streamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013 -c indexTableName=equinox-test-index

## To deploy local build of the Lambda (vs the latest nuget package)

    dotnet publish ../propulsion/Propulsion.DynamoStore.Indexer &&
    cdk deploy -c code=../propulsion/Propulsion.DynamoStore.Indexer/bin/Debug/net6.0/linux-arm64/publish -c dev/streamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013 -c dev/indexTableName=equinox-test-index

## To determine/verify the Streams ARN for a given index Table

    eqx stats dynamo -t equinox-test-index

## Useful commands

* `dotnet build`     check the CDK logic builds (no particular need to do this as `synth`/`deploy` triggers this implicitly)
* `cdk ls`           list all stacks in the app
* `cdk synth`        emits the synthesized CloudFormation template
* `cdk deploy`       deploy this stack to your default AWS account/region
* `cdk diff`         compare deployed stack with current state
* `cdk docs`         open CDK documentation
