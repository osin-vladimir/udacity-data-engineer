import argparse
import json

import boto3

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Creating AWS Redshift Cluster")
    parser.add_argument("--aws_profile", type=str)
    parser.add_argument("--dwh_db_name", default="udacity-de", type=str)
    parser.add_argument("--dwh_cluster_id", default="udacity-de", type=str)
    parser.add_argument("--dwh_db_user", default="dwh_user")
    parser.add_argument("--dwh_db_password", default="Passw0rd", type=str)
    parser.add_argument("--dhw_port", default=5439, type=int)
    parser.add_argument("--dwh_nodes", default=4, type=int)
    parser.add_argument("--dwh_s3_iam_name", default="redshift_s3_read_only")

    args = parser.parse_args()

    # create new boto3 session with profile credentials
    boto3.setup_default_session(profile_name=args.aws_profile)

    # create IAM role for RedShift
    iam = boto3.client("iam")
    try:
        redshift_s3_role = iam.create_role(RoleName="redshift_s3_read_only",
                                           Description="allows redshift to access s3",
                                           AssumeRolePolicyDocument=json.dumps(
                                               {
                                                   "Statement": [
                                                       {
                                                           "Action": "sts:AssumeRole",
                                                           "Effect": "Allow",
                                                           "Principal": {
                                                               "Service": "redshift.amazonaws.com"
                                                           }
                                                       }
                                                   ],
                                                   "Version": '2012-10-17'
                                               })
                                           )
    except iam.exceptions.EntityAlreadyExistsException:
        print("IAM role already exist")

    # attaching s3 read-only policy to created role
    iam.attach_role_policy(RoleName="redshift_s3_read_only",
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    # get role arn
    role_arn = iam.get_role(RoleName=args.dwh_s3_iam_name)['Role']['Arn']
    print("role_arn: ", role_arn)

    # creating redshift cluster
    redshift = boto3.client("redshift")

    try:
        response = redshift.create_cluster(
            # hardware
            ClusterType="multi-node",
            NodeType="dc2.large",
            NumberOfNodes=4,

            # credentials
            DBName=args.dwh_db_name,
            ClusterIdentifier=args.dwh_cluster_id,
            MasterUsername=args.dwh_db_user,
            MasterUserPassword=args.dwh_db_password,

            # roles
            IamRoles=[role_arn]
        )
    except redshift.exceptions.ClusterAlreadyExistsFault:
        print("Cluster already already exist")


