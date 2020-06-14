import argparse

import boto3

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Delete Redshift Cluster")
    parser.add_argument("--aws_profile", type=str)
    parser.add_argument("--dwh_cluster_id", default="udacity-de", type=str)
    parser.add_argument("--dwh_s3_iam_name", default="redshift_s3_read_only")
    args = parser.parse_args()

    # create new boto3 session with profile credentials
    boto3.setup_default_session(profile_name=args.aws_profile)

    # initialize boto3 clients
    redshift = boto3.client("redshift")
    iam = boto3.client("iam")

    # deleting redshift cluster
    redshift.delete_cluster(ClusterIdentifier=args.dwh_cluster_id,
                            SkipFinalClusterSnapshot=True)

    # detach role policy and remove role
    iam.detach_role_policy(RoleName=args.dwh_s3_iam_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=args.dwh_s3_iam_name)
