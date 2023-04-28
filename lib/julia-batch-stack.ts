import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  CustomState,
  Errors,
  JsonPath,
  Map,
  StateMachine,
} from "aws-cdk-lib/aws-stepfunctions";
import { LambdaInvoke } from "aws-cdk-lib/aws-stepfunctions-tasks";
import {
  DockerImageCode,
  DockerImageFunction,
  Runtime,
} from "aws-cdk-lib/aws-lambda";
import { Aws, Duration } from "aws-cdk-lib";
import { PythonFunction } from "@aws-cdk/aws-lambda-python-alpha";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";

export class JuliaBatchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const definition = new LambdaInvoke(this, "LambdaStart-Invoke", {
      lambdaFunction: new PythonFunction(this, "LambdaStart", {
        entry: "lib/lambda/start",
        runtime: Runtime.PYTHON_3_9,
      }),
    });

    const bucket = new Bucket(this, "Bucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const dummyMap = new Map(this, "DummyMap");

    const computeFunction = new PythonFunction(this, "LambdaMap-Compute", {
      entry: "lib/lambda/compute-map",
      runtime: Runtime.PYTHON_3_9,
      timeout: Duration.minutes(15),
      memorySize: 4092,
    });

    dummyMap.iterator(
      new LambdaInvoke(this, "LambdaMap-Compute-Invoke", {
        lambdaFunction: computeFunction,
      }).addRetry({
        errors: ["Lambda.TooManyRequestsException"],
        maxAttempts: 10,
      })
    );

    const distributedMap = new CustomState(this, "DistributedMap", {
      stateJson: {
        Type: "Map",
        // MaxConcurrency: 100,
        ItemsPath: "$.Payload.items",
        // ItemReader: {
        //   Resource: "arn:aws:states:::s3:getObject",
        //   ReaderConfig: {
        //     InputType: "CSV",
        //     CSVHeaderLocation: "FIRST_ROW",
        //   },
        //   Parameters: {
        //     Bucket: "some-bucket-name",
        //     "Key.$": "$.my_s3_key",
        //   },
        // },
        ItemSelector: {
          "index.$": "$$.Map.Item.Value",
          "size.$": "$.Payload.size",
        },
        ItemProcessor: {
          ...(dummyMap.toStateJson() as any).Iterator,
          ProcessorConfig: {
            Mode: "DISTRIBUTED",
            ExecutionType: "STANDARD",
          },
        },
        ResultWriter: {
          Resource: "arn:aws:states:::s3:putObject",
          Parameters: {
            Bucket: bucket.bucketName,
            Prefix: "process_output",
          },
        },
        ResultPath: "$.results",
      },
    });

    // const map = new Map(this, "Map", {
    //   itemsPath: JsonPath.stringAt("$.Payload.items"),

    //   resultPath: JsonPath.stringAt("$.results"),
    //   resultSelector: {
    //     "computed.$": "$.[*].Payload.result",
    //   },
    // });

    const sumFunction = new PythonFunction(this, "LambdaSum", {
      entry: "lib/lambda/sum",
      runtime: Runtime.PYTHON_3_9,
      timeout: Duration.minutes(15),
      memorySize: 4092,
    });
    definition.next(distributedMap).next(
      new LambdaInvoke(this, "LambdaSum-Invoke", {
        lambdaFunction: sumFunction,
      })
    );

    const sm = new StateMachine(this, "StateMachine", {
      definition,
      stateMachineName: "python-compute",
    });

    sm.addToRolePolicy(
      new PolicyStatement({
        actions: ["states:StartExecution"],
        effect: Effect.ALLOW,
        resources: [
          `arn:aws:states:${Aws.REGION}:${Aws.ACCOUNT_ID}:stateMachine:python-compute`,
        ],
      })
    );
    sm.addToRolePolicy(
      new PolicyStatement({
        actions: ["states:DescribeExecution", "states:StopExecution"],
        effect: Effect.ALLOW,
        resources: [
          `arn:aws:states:${Aws.REGION}:${Aws.ACCOUNT_ID}:execution:python-compute/*`,
        ],
      })
    );

    computeFunction.grantInvoke(sm);
    bucket.grantRead(sumFunction);
    bucket.grantReadWrite(sm);
  }
}
