#!/usr/bin/env python3

from aws_cdk import aws_lambda, core
from aws_cdk.aws_apigatewayv2 import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations import LambdaProxyIntegration
from aws_cdk.aws_lambda_python import PythonFunction


class LambdaApiStack(core.Stack):
    """
    """

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        lambda_function = PythonFunction(
            self,
            f"{id}-lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            entry='../openaq_fastapi',
            index='openaq_fastapi/main.py',
            allow_public_subnet=True,
            handler='handler',
            memory_size=1512,
            timeout=core.Duration.seconds(30),
            environment={},
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=LambdaProxyIntegration(handler=lambda_function),
            cors_preflight={
                "allow_headers": ["Authorization"],
                "allow_methods": [HttpMethod.GET, HttpMethod.HEAD, HttpMethod.OPTIONS, HttpMethod.POST],
                "allow_origins": ["*"],
                "max_age": core.Duration.days(10)
            }
        )

        core.CfnOutput(self, "Endpoint", value=api.url)


app = core.App()
stack = LambdaApiStack(app, "openaq-lcs-api")
core.Tags.of(stack).add("devseed", "true")
core.Tags.of(stack).add("lcs", "true")
app.synth()
