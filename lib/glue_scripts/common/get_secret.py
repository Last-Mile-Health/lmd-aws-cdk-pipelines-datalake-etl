import boto3
from botocore.exceptions import ClientError
from lib.configuration import (
    REGION
)

# Return (value: string|binary, isString: Bool)
