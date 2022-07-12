import os
import boto3
import botocore
from retrying import retry
import socket
import csv

def _is_retryable_exception(exception):
    return not isinstance(exception, botocore.exceptions.ClientError) or \
        (exception.response["Error"]["Code"] in ['LimitExceededException', 'RequestLimitExceeded', 'Throttling', 'ThrottlingException', 'ParamValidationError', 'ServerException', 'InternalServiceError'])

def _arn_to_name(resource_arn):
    # Example: arn:aws:elasticloadbalancing:us-east-1:397853141546:loadbalancer/pb-adn-arc2
    parts = resource_arn.split(':')
    name = parts[-1]
    parts = name.split('/', 1)
    if len(parts) == 2:
        name = parts[-1]

    return name

def _arn_to_route53_type(resource_arn: str) -> str:
    # Example: arn:aws:elasticloadbalancing:us-east-1:397853141546:loadbalancer/pb-adn-arc2
    parts: list = resource_arn.split(':')
    name: str = parts[-1]
    parts = name.split('/', 1)
    type = None
    if len(parts) == 2:
        type = parts[0]

    return type

def _arn_to_region(resource_arn: str) -> str:
    # Example: arn:aws:elasticloadbalancing:us-east-1:397853141546:loadbalancer/pb-adn-arc2
    parts: list = resource_arn.split(':')
    region = None
    if len(parts) > 3:
        region: str = parts[3]

    return region

def _format_dict(tags):
    output = []
    for (key, value) in tags.items():
        output.append("%s:%s" % (key, value))

    return ", ".join(output)

def _dict_to_aws_tags(tags, tagKeyValue=("Key", "Value")):
    tagKey, tagValue = tagKeyValue
    return [{tagKey: key, tagValue: value} for (key, value) in tags.items()]

def _aws_tags_to_dict(aws_tags):
    return {x['Key']: x['Value'] for x in aws_tags}

def _fetch_temporary_credentials(role):
    sts = boto3.client('sts', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

    response = sts.assume_role(RoleArn=role, RoleSessionName='aws-tagger.%s' % socket.gethostname())
    access_key_id = response.get('Credentials', {}).get('AccessKeyId', None)
    secret_access_key = response.get('Credentials', {}).get('SecretAccessKey', None)
    session_token = response.get('Credentials', {}).get('SessionToken', None)
    return access_key_id, secret_access_key, session_token

def _client(name, role, region):
    kwargs = {}
 
    if region:
        kwargs['region_name'] = region
    elif os.environ.get('AWS_REGION'):
        kwargs['region_name'] = os.environ['AWS_REGION']

    if role:
        access_key_id, secret_access_key, session_token = _fetch_temporary_credentials(role)
        kwargs['aws_access_key_id'] = access_key_id
        kwargs['aws_secret_access_key'] = secret_access_key
        kwargs['aws_session_token'] = session_token

    return boto3.client(name, **kwargs)

class SingleResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.taggers = {}
        self.taggers['airflow'] = AirflowTagger(dryrun, verbose, role=role, region=region)
        self.taggers['athena'] = AthenaTagger(dryrun, verbose, role=role, region=region)
 
        self.taggers['cloudfront'] = CloudfrontTagger(dryrun, verbose, role=role, region=region)
        self.taggers['codeartifact'] = CodeartifactTagger(dryrun, verbose, role=role, region=region)
        self.taggers['codebuild'] = CodebuildTagger(dryrun, verbose, role=role, region=region)
        self.taggers['codepipeline'] = CodepipelineTagger(dryrun, verbose, role=role, region=region)
        self.taggers['cognito-idp'] = CognitoIdpTagger(dryrun, verbose, role=role, region=region)

        self.taggers['dynamodb'] = DynamoDBTagger(dryrun, verbose, role=role, region=region)

        self.taggers['ec2'] = EC2Tagger(dryrun, verbose, role=role, region=region, tag_volumes=tag_volumes)
        self.taggers['ecr'] = ECRTagger(dryrun, verbose, role=role, region=region)
        self.taggers['ecs'] = ECSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['elasticache'] = ElasticacheTagger(dryrun, verbose, role=role, region=region)
        self.taggers['elasticfilesystem'] = EFSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['elasticloadbalancing'] = LBTagger(dryrun, verbose, role=role, region=region)
        self.taggers['es'] = ESTagger(dryrun, verbose, role=role, region=region)

        self.taggers['geo'] = LocationTagger(dryrun, verbose, role=role, region=region)
        self.taggers['glue'] = GlueTagger(dryrun, verbose, role=role, region=region)

        self.taggers['kinesis'] = KinesisTagger(dryrun, verbose, role=role, region=region)
        self.taggers['kms'] = KMSTagger(dryrun, verbose, role=role, region=region)

        self.taggers['lambda'] = LambdaTagger(dryrun, verbose, role=role, region=region)
        self.taggers['logs'] = CloudWatchLogsTagger(dryrun, verbose, role=role, region=region)

        self.taggers['r53resolver'] = R53ResolverTagger(dryrun, verbose, role=role, region=region)
        self.taggers['redshift'] = RedshiftTagger(dryrun, verbose, role=role, region=region)
        self.taggers['rds'] = RDSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['route53'] = Route53Tagger(dryrun, verbose, role=role, region=region)

        self.taggers['s3'] = S3Tagger(dryrun, verbose, role=role, region=region)
        self.taggers['sagemaker'] = SagemakerTagger(dryrun, verbose, role=role, region=region)
        self.taggers['secretsmanager'] = SecretsmanagerTagger(dryrun, verbose, role=role, region=region)
        self.taggers['sqs'] = SQSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['sns'] = SNSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['states'] = StatesTagger(dryrun, verbose, role=role, region=region)

        self.taggers['wafv2'] = WAFv2Tagger(dryrun, verbose, role=role, region=region)


    def tag(self, resource_id, tags):
        if resource_id == "":
            return

        if len(tags) == 0:
            return

        tagger = None
        resource_arn = resource_id
        if resource_id.startswith('arn:'):
            product, resource_id = self._parse_arn(resource_id)
            if product:
                tagger = self.taggers.get(product)
        else:
            tagger = self.taggers['s3']


        if resource_id.startswith('i-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('vol-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        # elif resource_id.startswith('tgw-'):
        #     tagger = self.taggers['ec2']
        #     resource_arn = resource_id
        elif resource_id.startswith('snap-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id

        if tagger:
            tagger.tag(resource_arn, tags)
        else:
            print("Tagging is not support for this resource %s\n" % resource_id)

    def _parse_arn(self, resource_arn):
        product = None
        resource_id = None
        parts = resource_arn.split(':')
        if len(parts) > 5:
            product = parts[2]
            resource_id = parts[5]
            resource_parts = resource_id.split('/')
            if len(resource_parts) > 1:
                resource_id = resource_parts[-1]

        return product, resource_id

class MultipleResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.tagger = SingleResourceTagger(dryrun, verbose, role=role, region=region, tag_volumes=tag_volumes)

    def tag(self, resource_ids, tags):
        for resource_id in resource_ids:
            self.tagger.tag(resource_id, tags)

class CSVResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.dryrun = dryrun
        self.verbose = verbose
        self.tag_volumes = tag_volumes
        self.role = role
        self.region = region
        self.regional_tagger = {}
        self.resource_id_column = 'Id'
        self.region_column = 'Region'

    def tag(self, filename):
        with open(filename, 'rU') as csv_file:
            reader = csv.reader(csv_file)
            header_row = True
            tag_index = None

            for row in reader:
                if header_row:
                    header_row = False
                    tag_index = self._parse_header(row)
                else:
                    self._tag_resource(tag_index, row)

    def _parse_header(self, header_row):
        tag_index = {}
        for index, name in enumerate(header_row):
            tag_index[name] = index

        return tag_index

    def _tag_resource(self, tag_index, row):
        resource_id = row[tag_index[self.resource_id_column]]
        tags = {}
        for (key, index) in tag_index.items():
            value = row[index]
            if key != self.resource_id_column and key != self.region_column and value != "":
                tags[key] = value

        tagger = self._lookup_tagger(tag_index, row)
        tagger.tag(resource_id, tags)

    def _lookup_tagger(self, tag_index, row):
        region = self.region
        region_index = tag_index.get(self.region_column)
        resource_id = row[tag_index[self.resource_id_column]]

        if region_index is not None:
            region = row[region_index]
        if _arn_to_region(resource_arn=resource_id):
            region = _arn_to_region(resource_arn=resource_id)
        if region == '':
            region = None

        tagger = self.regional_tagger.get(region)
        if tagger is None:
            tagger = SingleResourceTagger(self.dryrun, self.verbose, role=self.role, region=region, tag_volumes=self.tag_volumes)
            self.regional_tagger[region] = tagger

        return tagger

class EC2Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.dryrun = dryrun
        self.verbose = verbose
        self.ec2 = _client('ec2', role=role, region=region)
        self.volume_cache = {}
        if tag_volumes:
            self.add_volume_cache()

    def add_volume_cache(self):
        #TODO implement paging for describe instances
        reservations = self._ec2_describe_instances(MaxResults=1000)

        for reservation in reservations["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance['InstanceId']
                volumes = instance.get('BlockDeviceMappings', [])
                self.volume_cache[instance_id] = []
                for volume in volumes:
                    ebs = volume.get('Ebs', {})
                    volume_id = ebs.get('VolumeId')
                    if volume_id:
                        self.volume_cache[instance_id].append(volume_id)

    def tag(self, instance_id, tags):
        aws_tags = _dict_to_aws_tags(tags)
        resource_ids = [_arn_to_name(instance_id)]
        resource_ids.extend(self.volume_cache.get(instance_id, []))
        if self.verbose:
            print("tagging %s with %s" % (", ".join(resource_ids), _format_dict(tags)))
        if not self.dryrun:
            try:
                self._ec2_create_tags(Resources=resource_ids, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['InvalidSnapshot.NotFound', 'InvalidVolume.NotFound', 'InvalidInstanceID.NotFound', 'InvalidVpcEndpointId.NotFound', 'InvalidGroup.NotFound']:
                    print("Resource not found: %s\n" % instance_id)
                else:
                    raise exception


    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ec2_describe_instances(self, **kwargs):
        return self.ec2.describe_instances(**kwargs)

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ec2_create_tags(self, **kwargs):
        return self.ec2.create_tags(**kwargs)

class EFSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.efs = _client('efs', role=role, region=region)

    def tag(self, resource_arn, tags):
        file_system_id = _arn_to_name(resource_arn)
        aws_tags = _dict_to_aws_tags(tags)

        if self.verbose:
            print("tagging %s with %s" % (file_system_id, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._efs_create_tags(FileSystemId=file_system_id, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['FileSystemNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _efs_create_tags(self, **kwargs):
        return self.efs.create_tags(**kwargs)

class DynamoDBTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.dynamodb = _client('dynamodb', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._dynamodb_tag_resource(ResourceArn=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _dynamodb_tag_resource(self, **kwargs):
        return self.dynamodb.tag_resource(**kwargs)


class LambdaTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.alambda = _client('lambda', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._lambda_tag_resource(Resource=resource_arn, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['ValidationException']:
                    print("Validation error on resource: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _lambda_tag_resource(self, **kwargs):
        return self.alambda.tag_resource(**kwargs)


class CloudWatchLogsTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.logs= _client('logs', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        log_group = None
        parts = resource_arn.split(':')
        if len(parts) > 0:
            log_group = parts[-1]

        if not log_group:
            print("Invalid ARN format for CloudWatch Logs: %s\n" % resource_arn)
            return

        if not self.dryrun:
            try:
                self._logs_tag_log_group(logGroupName=log_group, tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _logs_tag_log_group(self, **kwargs):
        return self.logs.tag_log_group(**kwargs)


class RDSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.rds = _client('rds', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._rds_add_tags_to_resource(ResourceName=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['DBInstanceNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _rds_add_tags_to_resource(self, **kwargs):
        return self.rds.add_tags_to_resource(**kwargs)

class LBTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.elb = _client('elb', role=role, region=region)
        self.alb = _client('elbv2', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)

        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                if ':loadbalancer/app/' in resource_arn:
                    self._alb_add_tags(ResourceArns=[resource_arn], Tags=aws_tags)
                else:
                    elb_name = _arn_to_name(resource_arn)
                    self._elb_add_tags(LoadBalancerNames=[elb_name], Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['LoadBalancerNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _elb_add_tags(self, **kwargs):
        return self.elb.add_tags(**kwargs)

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _alb_add_tags(self, **kwargs):
        return self.alb.add_tags(**kwargs)

class KinesisTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.kinesis = _client('kinesis', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                stream_name = _arn_to_name(resource_arn)
                self._kinesis_add_tags_to_stream(StreamName=stream_name, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _kinesis_add_tags_to_stream(self, **kwargs):
        return self.kinesis.add_tags_to_stream(**kwargs)

class ESTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.es = _client('es', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._es_add_tags(ARN=resource_arn, TagList=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ValidationException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _es_add_tags(self, **kwargs):
        return self.es.add_tags(**kwargs)

class ElasticacheTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.elasticache = _client('elasticache', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._elasticache_add_tags_to_resource(ResourceName=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['CacheClusterNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _elasticache_add_tags_to_resource(self, **kwargs):
        return self.elasticache.add_tags_to_resource(**kwargs)

class CloudfrontTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.cloudfront = _client('cloudfront', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._cloudfront_tag_resource(Resource=resource_arn, Tags={'Items': aws_tags})
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['NoSuchResource']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _cloudfront_tag_resource(self, **kwargs):
        return self.cloudfront.tag_resource(**kwargs)

class S3Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.s3 = _client('s3', role=role, region=region)

    def tag(self, bucket_name, tags):
        try:
            if bucket_name.startswith('arn:'):
                bucket_name = _arn_to_name(bucket_name)
            response = self._s3_get_bucket_tagging(Bucket=bucket_name)
            # add existing tags
            for (key, value) in _aws_tags_to_dict(response.get('TagSet', [])).items():
                if key not in tags:
                    tags[key] = value
        except botocore.exceptions.ClientError as exception:
            if exception.response["Error"]["Code"] in ['NoSuchTagSet', 'NoSuchBucket']:
                print("Resource not found: %s\n" % bucket_name)
            elif exception.response["Error"]["Code"] in ['AccessDenied']:
                print("Access to resource denied: %s\n" % bucket_name)
            else:
                raise exception

        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (bucket_name, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._s3_put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': aws_tags})
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['NoSuchBucket']:
                    print("Resource not found: %s\n" % bucket_name)
                elif exception.response["Error"]["Code"] in ['AccessDenied']:
                    print("Access to resource denied: %s\n" % bucket_name)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _s3_get_bucket_tagging(self, **kwargs):
        return self.s3.get_bucket_tagging(**kwargs)

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _s3_put_bucket_tagging(self, **kwargs):
        return self.s3.put_bucket_tagging(**kwargs)


class CodeartifactTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.codeartifact = _client('codeartifact', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags, ("key", "value"))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._codeartifact_tag_resource(resourceArn=resource_arn, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['AccessDeniedException']:
                    print("User doesn't have access to resource modification: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['ServiceQuotaExceededException']:
                    print("The operation did not succeed because it would have exceeded a service limit for your account: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _codeartifact_tag_resource(self, **kwargs):
        return self.codeartifact.tag_resource(**kwargs)


class KMSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.kms = _client('kms', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags, ("TagKey", "TagValue"))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._kms_tag_resource(KeyId=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['NotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['AccessDeniedException']:
                    print("User doesn't have access to resource modification: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['LimitExceededException']:
                    print("The request was rejected because a quota was exceeded: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _kms_tag_resource(self, **kwargs):
        return self.kms.tag_resource(**kwargs)


class RedshiftTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.redshift = _client('redshift', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._redshift_tag_resource(ResourceName=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundFault']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['TagLimitExceededFault']:
                    print("You have exceeded the number of tags allowed: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _redshift_tag_resource(self, **kwargs):
        return self.redshift.create_tags(**kwargs)


class SNSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.sns = _client('sns', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._sns_tag_resource(ResourceArn=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['TagLimitExceeded']:
                    print("Can't add more than 50 tags to a topic: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _sns_tag_resource(self, **kwargs):
        return self.sns.tag_resource(**kwargs)


class WAFv2Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.wafv2 = _client('wafv2', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._wafv2_tag_resource(ResourceARN=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['WAFNonexistentItemException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _wafv2_tag_resource(self, **kwargs):
        return self.wafv2.tag_resource(**kwargs)


class AirflowTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.airflow = _client('mwaa', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._airflow_tag_resource(ResourceArn=resource_arn, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _airflow_tag_resource(self, **kwargs):
        return self.airflow.tag_resource(**kwargs)


class ECRTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.ecr = _client('ecr', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._ecr_tag_resource(resourceArn=resource_arn, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['RepositoryNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['TooManyTagsException']:
                    print("The list of tags on the repository is over the limit: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ecr_tag_resource(self, **kwargs):
        return self.ecr.tag_resource(**kwargs)


class ECSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.ecs = _client('ecs', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags, ("key", "value"))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._ecs_tag_resource(resourceArn=resource_arn, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException', 'ClusterNotFoundException', 'InvalidParameterException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ecs_tag_resource(self, **kwargs):
        return self.ecs.tag_resource(**kwargs)


class LocationTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.location = _client('location', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._location_tag_resource(ResourceArn=resource_arn, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['NoSuchResource']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _location_tag_resource(self, **kwargs):
        return self.location.tag_resource(**kwargs)


class GlueTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.glue = _client('glue', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._glue_tag_resource(ResourceArn=resource_arn, TagsToAdd=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['EntityNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['AccessDeniedException']:
                    print("Access to resource denied: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _glue_tag_resource(self, **kwargs):
        return self.glue.tag_resource(**kwargs)


class R53ResolverTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.r53resolver = _client('route53resolver', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._r53resolver_tag_resource(ResourceArn=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['AccessDeniedException']:
                    print("Access to resource denied: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _r53resolver_tag_resource(self, **kwargs):
        return self.r53resolver.tag_resource(**kwargs)


class AthenaTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.athena = _client('athena', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._athena_tag_resource(ResourceARN=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _athena_tag_resource(self, **kwargs):
        return self.athena.tag_resource(**kwargs)


class CodebuildTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.codebuild = _client('codebuild', role=role, region=region)

    def tag(self, resource_arn, tags):
        codebuild_project_name = _arn_to_name(resource_arn)
        aws_tags = _dict_to_aws_tags(tags, ("key", "value"))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._codebuild_tag_resource(name=codebuild_project_name, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _codebuild_tag_resource(self, **kwargs):
        return self.codebuild.update_project(**kwargs)


class CodepipelineTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.codepipeline = _client('codepipeline', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags, ('key', 'value'))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._codepipeline_tag_resource(resourceArn=resource_arn, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['TooManyTagsException']:
                    print("The tags limit for a resource has been exceeded: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _codepipeline_tag_resource(self, **kwargs):
        return self.codepipeline.tag_resource(**kwargs)



class CognitoIdpTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.cognitoidp = _client('cognito-idp', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._cognitoidp_tag_resource(ResourceArn=resource_arn, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _cognitoidp_tag_resource(self, **kwargs):
        return self.cognitoidp.tag_resource(**kwargs)


class Route53Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.route53 = _client('route53', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        resource_type = _arn_to_route53_type(resource_arn)
        resource_id = _arn_to_name(resource_arn)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._route53_tag_resource(ResourceId=resource_id, ResourceType=resource_type, AddTags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException', 'ResourceNotFound', 'NoSuchResource']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['NoSuchHostedZone']:
                    print("No hosted zone exists with the ID that you specified. %s\n" % resource_id)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _route53_tag_resource(self, **kwargs):
        return self.route53.change_tags_for_resource(**kwargs)


class SecretsmanagerTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.secretsmanager = _client('secretsmanager', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._secretsmanager_tag_resource(SecretId=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _secretsmanager_tag_resource(self, **kwargs):
        return self.secretsmanager.tag_resource(**kwargs)


class SQSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.sqs = _client('sqs', role=role, region=region)

    def tag(self, resource_arn, tags):
        # aws_tags = _dict_to_aws_tags(tags)
        queue_name = _arn_to_name(resource_arn)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                queue_url = self._get_queue_url(QueueName=queue_name)['QueueUrl']
                self._sqs_tag_resource(QueueUrl=queue_url, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    def _get_queue_url(self, **kwargs):
        return self.sqs.get_queue_url(**kwargs)

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _sqs_tag_resource(self, **kwargs):
        return self.sqs.tag_queue(**kwargs)


class StatesTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.states = _client('stepfunctions', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags, ('key', 'value'))
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._states_tag_resource(resourceArn=resource_arn, tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                elif exception.response["Error"]["Code"] in ['TooManyTags']:
                    print("You've exceeded the number of tags allowed for a resource: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _states_tag_resource(self, **kwargs):
        return self.states.tag_resource(**kwargs)



class SagemakerTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.sagemaker = _client('sagemaker', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = _dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, _format_dict(tags)))
        if not self.dryrun:
            try:
                self._sagemaker_tag_resource(ResourceArn=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFound']:
                    print("Resource not found: %s\n" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=_is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _sagemaker_tag_resource(self, **kwargs):
        return self.sagemaker.add_tags(**kwargs)
