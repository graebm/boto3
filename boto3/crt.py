from awscrt.auth import AwsCredentials
from awscrt.s3 import CrossProcessLock

from s3transfer.crt import (
    create_s3_crt_client, BotocoreCRTRequestSerializer, CRTTransferManager
)

from botocore.session import Session


class CRTS3TransferManager:

    def __init__(self, crt_s3_client, process_lock, region):
        self._crt_s3_client = crt_s3_client
        self._process_lock = process_lock
        self._region = region


class CRTCredentialWrapper:
    def __init__(self, resolved_credentials):
        self._resolved_credentials = resolved_credentials

    def __call__(self):
        credentials = self._get_credentials().get_frozen_credentials()
        return AwsCredentials(
            credentials.access_key, credentials.secret_key, credentials.token
        )

    def _get_credentials(self):
        if self._resolved_credentials is None:
            raise NoCredentialsError()
        return self._resolved_credentials

    def load_credentials(self):
        return self._get_credentials()


def initialize_crt_s3_transfer_manager(client, config):
    try: 
        lock = CrossProcessLock('boto3')
        lock.acquire()
    except RuntimeError:
        # If we're unable to acquire the lock, we cannot
        # use the CRT in this process and should default to
        # the default s3transfer manager.
        return None

    return CRTS3TransferManager(
        create_crt_transfer_manager(client, config),
        lock,
        client.meta.region_name
    )
    

def create_crt_transfer_manager(client, config):
    """Create a CRTTransferManager for optimized data transfer.

    This function is considered an internal detail for boto3
    and should not be invoked in external code. It is subject to
    abrupt breaking changes.
    """
    sess = Session()
    region = client.meta.region_name
    cred_provider = CRTCredentialWrapper(client._get_credentials())
    return CRTTransferManager(
        _create_crt_client(sess, config, region, cred_provider),
        _create_crt_request_serializer(sess, region)
    )


def _create_crt_client(session, config, region_name, cred_provider):
    create_crt_client_kwargs = {
        'region': region_name,
        'verify': False,
        'use_ssl': True,
    }
    # TODO: We have no way to get the endpoint_url off a client
    # endpoint_url = ???

    target_throughput = config.max_bandwidth
    multipart_chunksize = config.multipart_chunksize
    if target_throughput:
        create_crt_client_kwargs['target_throughput'] = target_throughput
    if multipart_chunksize:
        create_crt_client_kwargs['part_size'] = multipart_chunksize

    # TODO: How do we do this properly?
    create_crt_client_kwargs['botocore_credential_provider'] = cred_provider

    return create_s3_crt_client(**create_crt_client_kwargs)


def _create_crt_request_serializer(session, region_name):
    return BotocoreCRTRequestSerializer(
        session,
        {
            'region_name': region_name,
            'endpoint_url': None, # TODO: We don't have a way to get an endpoint_url
        }
    )

