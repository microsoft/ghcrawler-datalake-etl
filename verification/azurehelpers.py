"""azurehelpers.py - helper functions for common Azure tasks

Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.
"""

from dougerino import setting

from azure.storage.blob import BlockBlobService
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.datalake.store import core, lib, multithread

def azure_blob_dir(az_acct, az_key, az_container): #-------------------------<<<
    """Return list of blobnames for an Azure storage container.

    az_acct = Azure account
    az_key = access key for the account
    az_container = Azure blob storage container name
    """
    block_blob_service = BlockBlobService(account_name=az_acct,
                                          account_key=az_key)
    return block_blob_service.list_blobs(az_container)

def azure_blob_get(az_acct, az_key, #----------------------------------------<<<
                   az_container, az_blob, filename):
    """Download a blob from an Azure storage container.

    az_acct = Azure account
    az_key = access key for the account
    az_container = Azure blob storage container name
    az_blob = blobname within the container
    filename = local filename to which the blob will be saved
    """
    block_blob_service = BlockBlobService(account_name=az_acct, account_key=az_key)
    block_blob_service.get_blob_to_path(az_container, az_blob, filename)

def azure_datalake_token(inifile): #-----------------------------------------<<<
    """Return token and credentials for AAD authentication in Azure Data Lake.

    inifile = the name of an ini file (in ../_private, as used by settings())
              that contains an [aad] section with these values: tenant-id,
              client-secret, client-id
    """
    tenantid = setting(topic=inifile, section='aad', key='tenant-id')
    clientsecret = setting(topic=inifile, section='aad', key='client-secret')
    clientid = setting(topic=inifile, section='aad', key='client-id')
    return (
        lib.auth(tenant_id=tenantid,
                 client_secret=clientsecret,
                 client_id=clientid),
        ServicePrincipalCredentials(client_id=clientid,
                                    secret=clientsecret,
                                    tenant=tenantid))

def datalake_dir(adls_account, adls_token, folder): #------------------------<<<
    """Get a directory for an ADL filesystem folder.

    adls_account = name of an ADLS account (e.g., 'ghinsightsms')
    adls_token = ADLS access token (as returned by azure_datalake_token())
    folder = name of a folder in the ADLS account

    Returns sorted list of the filenames in the folder.
    """
    adls_fs_client = \
        core.AzureDLFileSystem(adls_token, store_name=adls_account)
    return sorted([filename.split('/')[1] for
                   filename in adls_fs_client.listdir(folder)],
                  key=lambda fname: fname.lower())

def datalake_get_file(localfile, remotefile, adls_acct, token): #------------<<<
    """Download a file from Azure Data Lake Store.
    """
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_acct)

    multithread.ADLDownloader(adls_fs_client, lpath=localfile,
                              rpath=remotefile, nthreads=64, overwrite=True,
                              buffersize=4194304, blocksize=4194304)

def datalake_put_file(localfile, remotefile, adls_acct, token): #------------<<<
    """Upload a file to an Azure Data Lake Store.

    Note that the remote filename should be relative to the root.
    For example: '/users/dmahugh/repo_diff.csv'
    """
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_acct)

    multithread.ADLUploader(adls_fs_client, lpath=localfile,
                            rpath=remotefile, nthreads=64, overwrite=True,
                            buffersize=4194304, blocksize=4194304)

