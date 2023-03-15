## Imports

# From standard library
import json

# From third party
import boto3
import botocore
from dotenv import dotenv_values

# IAM Role Global Variables
ROLE_NAME = "dwhRole"
DESCRIPTION = "Allows Redshift clusters to call AWS services on your behalf."
ASSUME_ROLE_POLICY_DOCUMENT = json.dumps(
    {
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
            }
        ],
        "Version": "2012-10-17",
    }
)
POLICY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"  # For attaching policies

# Redshift Global Variables
CLUSTER_IDENTIFIER = "dwhCluster"
CLUSTER_TYPE = "multi-node"
NUM_NODES = 4
NODE_TYPE = "dc2.large"
DB_NAME = "dwh"
MASTER_USER_NAME = "dwhuser"
MASTER_USER_PASSWORD = "Passw0rd"

# Connection Global Variables
PORT = 5439
CIDR_IP = "0.0.0.0/0"
IP_PROTOCOL = "TCP"


env = dotenv_values()

AWS_KEY = env["AWS_KEY"]
AWS_SECRET = env["AWS_SECRET"]


def _get_ressources(aws_key, aws_secret):
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2",
    )
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2",
    )
    return ec2, s3


def _get_clients(aws_key, aws_secret):
    redshift = boto3.client(
        "redshift",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2",
    )
    iam = boto3.client(
        "iam",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="us-west-2",
    )
    return iam, redshift


def _create_role(iam, role_name):
    try:
        iam.create_role(
            RoleName=role_name,
            Descprition="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )

    except Exception as e:
        print(e)


class Infra:
    def __init__(self, aws_key, aws_secret):
        self.ec2, self.s3 = _get_ressources(aws_key=aws_key, aws_secret=aws_secret)
        self.iam, self.redshift = _get_clients(aws_key=aws_key, aws_secret=aws_secret)


def main():
    infra = Infra(aws_key=AWS_KEY, aws_secret=AWS_SECRET)
    infra.iam.get_role(RoleName=ROLE_NAME)


if __name__ == "__main__":
    main()
