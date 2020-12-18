from aws_cdk import aws_lambda, aws_s3, core, aws_iam
from aws_cdk.aws_apigatewayv2 import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations import LambdaProxyIntegration
from aws_cdk.aws_lambda_event_sources import S3EventSource
from aws_cdk.aws_lambda_python import PythonFunction

from openaq_fastapi.settings import settings


def dictstr(item):
    return item[0], str(item[1])


env = dict(map(dictstr, settings.dict().items()))
print(env)


class LambdaApiStack(core.Stack):
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
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            entry="../openaq_fastapi",
            index="openaq_fastapi/main.py",
            allow_public_subnet=True,
            handler="handler",
            memory_size=1512,
            timeout=core.Duration.seconds(30),
            environment=env,
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=LambdaProxyIntegration(
                handler=lambda_function
            ),
            cors_preflight={
                "allow_headers": [
                    "Authorization",
                    "*",
                ],
                "allow_methods": [
                    HttpMethod.GET,
                    HttpMethod.HEAD,
                    HttpMethod.OPTIONS,
                    HttpMethod.POST,
                ],
                "allow_origins": ["*"],
                "max_age": core.Duration.days(10),
            },
        )

        # ingest_function = PythonFunction(
        #     self,
        #     f"{id}-ingest-lambda",
        #     runtime=aws_lambda.Runtime.PYTHON_3_8,
        #     entry="../openaq_fastapi",
        #     index="openaq_fastapi/ingest.py",
        #     allow_public_subnet=True,
        #     handler="handler",
        #     memory_size=1512,
        #     environment=env,
        # )

        # ingest_function.add_permission(
        #     "s3-service-principal",
        #     principal=aws_iam.ServicePrincipal("s3.amazonaws.com"),
        # )

        # openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
        #     self, "{id}-OPENAQ-FETCH-BUCKET", settings.OPENAQ_FETCH_BUCKET
        # )

        # ingest_function.add_event_source(
        #     S3EventSource(
        #         openaq_fetch_bucket,
        #         events=[aws_s3.EventType.Object_CREATED],
        #         filters=[
        #             aws_s3.aws_s3.NotificationKeyFilter(
        #                 prefix="realtime-gzipped/",
        #                 suffix=".ndjson.gz",
        #             )
        #         ],
        #     )
        # )

        # openaq_etl_bucket = aws_s3.Bucket.from_bucket_name(
        #     self, "{id}-OPENAQ-ETL-BUCKET", settings.OPENAQ_ETL_BUCKET
        # )

        core.CfnOutput(self, "Endpoint", value=api.url)


app = core.App()
print(f"openaq-lcs-api{settings.OPENAQ_ENV}")
stack = LambdaApiStack(app, f"openaq-lcs-api{settings.OPENAQ_ENV}")
core.Tags.of(stack).add("devseed", "true")
core.Tags.of(stack).add("lcs", "true")
app.synth()
