import argparse

import boto3

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Adding security group for default VPC")
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

    # creating redshift cluster
    redshift = boto3.client("redshift")

    # get params from created cluster
    cluster = redshift \
        .describe_clusters(ClusterIdentifier=args.dwh_cluster_id)['Clusters'][0]

    print(f"Cluster Endpoint: {cluster['Endpoint']['Address']}")

    # open an incoming TCP port to access the cluster endpoint
    ec2 = boto3.resource('ec2')

    try:
        vpc = ec2.Vpc(id=cluster['VpcId'])
        default_security_group = list(vpc.security_groups.all())[0]

        if default_security_group.group_name == "default":

            default_security_group.authorize_ingress(
                GroupName=default_security_group.group_name,
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                     'FromPort': args.dhw_port,
                     'ToPort': args.dhw_port,
                     'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
                ]
            )
    except Exception as e:
        print(e)
