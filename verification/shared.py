"""shared.py
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.
"""
# shared code used by the data verification programs
import configparser
import csv
import datetime
import gzip
import json
import os
import re

import requests

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

def days_since(datestr): #---------------------------------------------------<<<
    """Return # days since a date in YYYY-MM-DD format.
    """
    return (datetime.datetime.today() -
            datetime.datetime.strptime(datestr, '%Y-%m-%d')).days

def dicts2csv(listobj, filename): #------------------------------------------<<<
    """Write list of dictionaries to a CSV file.

    1st parameter = the list of dictionaries
    2nd parameter = name of CSV file to be written
    """
    csvfile = open(filename, 'w', newline='')

    # note that we assume all dictionaries in the list have the same keys
    csvwriter = csv.writer(csvfile, dialect='excel')
    header_row = [key for key, _ in listobj[0].items()]
    csvwriter.writerow(header_row)

    for row in listobj:
        values = []
        for fldname in header_row:
            values.append(row[fldname])
        csvwriter.writerow(values)

    csvfile.close()

def documentation_repo(reponame): #------------------------------------------<<<
    """Check repo name for whether it appears to be a documentation repo.
    """
    if '-pr_' in reponame.lower() or '-pr.' in reponame.lower():
        return True # a hack to exclude a bunch of Microsoft repos
    docrepos = [r'.*-pr\..{2}-.{2}.*', r'.*\..{2}-.{2}.*', r'.*-pr$',
                r'.*\.handoff.*', r'.*\.handback', r'ontent-{4}\/']
    for regex in docrepos:
        if re.match(regex, reponame):
            return True

    return False

def filesize(filename): #----------------------------------------------------<<<
    """Return byte size of specified file.
    """
    return os.stat(filename).st_size

def get_asofdate(): #--------------------------------------------------------<<<
    """Get the most recent complete day prior to the timestamp of the Repo.csv
    file in Azure Data Lake Store.
    """
    token, _ = azure_datalake_token('ghinsights')
    adls_account = setting('ghinsights', 'azure', 'adls-account')
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_account)
    filename = '/TabularSource2/Repo.csv'
    # get the timestamp (seconds) for this file in ADLS file system ...
    timestamp_seconds = int(adls_fs_client.info(filename)['modificationTime'])/1000
    # convert that to a datetime object ...
    timestamp = datetime.datetime.fromtimestamp(timestamp_seconds)
    # subtract a day ...
    asof_datetime = timestamp - datetime.timedelta(days=1)
    # return first 10 characters as a string (YYYY-MM-DD) ...
    return str(asof_datetime)[:10]

def github_allpages(endpoint=None, auth=None, #------------------------------<<<
                    headers=None, state=None, session=None):

    """Get data from GitHub REST API.

    endpoint     = HTTP endpoint for GitHub API call
    headers      = HTTP headers to be included with API call

    Returns the data as a list of dictionaries. Pagination is handled by this
    function, so the complete data set is returned.
    """
    headers = {} if not headers else headers

    payload = [] # the full data set (all fields, all pages)
    page_endpoint = endpoint # endpoint of each page in the loop below

    while True:
        response = github_rest_api(endpoint=page_endpoint, auth=auth, \
            headers=headers, state=state, session=session)
        if (state and state.verbose) or response.status_code != 200:
            # note that status code is always displayed if not 200/OK
            print('>>> endpoint: {0}'.format(endpoint))
            print('      Status: {0}, {1} bytes returned'. \
                format(response, len(response.text)))
        if response.ok:
            thispage = json.loads(response.text)
            payload.extend(thispage)

        pagelinks = github_pagination(response)
        page_endpoint = pagelinks['nextURL']
        if not page_endpoint:
            break # no more results to process

    return payload

def github_pagination(link_header): #----------------------------------------<<<
    """Parse values from the 'link' HTTP header returned by GitHub API.

    1st parameter = either of these options ...
                    - 'link' HTTP header passed as a string
                    - response object returned by requests library

    Returns a dictionary with entries for the URLs and page numbers parsed
    from the link string: firstURL, firstpage, prevURL, prevpage, nextURL,
    nextpage, lastURL, lastpage.
    """
    # initialize the dictionary
    retval = {'firstpage':0, 'firstURL':None, 'prevpage':0, 'prevURL':None,
              'nextpage':0, 'nextURL':None, 'lastpage':0, 'lastURL':None}

    if isinstance(link_header, str):
        link_string = link_header
    else:
        # link_header is a response object, get its 'link' HTTP header
        try:
            link_string = link_header.headers['Link']
        except KeyError:
            return retval # no Link HTTP header found, nothing to parse

    links = link_string.split(',')
    for link in links:
        # link format = '<url>; rel="type"'
        linktype = link.split(';')[-1].split('=')[-1].strip()[1:-1]
        url = link.split(';')[0].strip()[1:-1]
        pageno = url.split('?')[-1].split('=')[-1].strip()

        retval[linktype + 'page'] = pageno
        retval[linktype + 'URL'] = url

    return retval

def github_rest_api(*, endpoint=None, auth=None, headers=None, #-------------<<<
                    state=None, session=None):
    """Call the GitHub API.

    endpoint     = the HTTP endpoint to call; if endpoint starts with / (for
                   example, '/orgs/microsoft'), it will be appended to
                   https://api.github.com
    auth         = optional authentication tuple - (username, pat)
                   If not specified, the default gitHub account is
                   setting('dougerino', 'defaults', 'github_user')
    headers      = optional dictionary of HTTP headers to pass
    state        = optional state object, where settings such as the session
                   object are stored. If provided, must have properties as used
                   below.
    session      = optional Requests session object reference. If not provided,
                   state.requests_session is the default session object. Use
                   the session argument to override that default and use a
                   different session. Use of a session object improves
                   performance.

    Returns the response object.

    Sends the Accept header to use version V3 of the GitHub API. This can
    be explicitly overridden by passing a different Accept header if desired.
    """
    if not endpoint:
        print('ERROR: github_api() called with no endpoint')
        return None

    # set auth to default if needed
    if not auth:
        default_account = setting('dougerino', 'defaults', 'github_user')
        if default_account:
            auth = (default_account, setting('github', default_account, 'pat'))
        else:
            auth = () # no auth specified, and no default account found

    # add the V3 Accept header to the dictionary
    headers = {} if not headers else headers
    headers_dict = {**{"Accept": "application/vnd.github.v3+json"}, **headers}

    # make the API call
    if session:
        sess = session # explictly passed Requests session
    elif state:
        if state.requests_session:
            sess = state.requests_session # Requests session on the state objet
        else:
            sess = requests.session() # create a new Requests session
            state.requests_session = sess # save it in the state object
    else:
        # if no state or session specified, create a temporary Requests
        # session to use below. Note it's not saved/re-used in this scenario
        # so performance won't be optimized.
        sess = requests.session()

    sess.auth = auth
    full_endpoint = 'https://api.github.com' + endpoint if endpoint[0] == '/' \
        else endpoint
    response = sess.get(full_endpoint, headers=headers_dict)

    if state and state.verbose:
        print('    Endpoint: ' + endpoint)

    if state:
        # update rate-limit settings
        try:
            state.last_ratelimit = int(response.headers['X-RateLimit-Limit'])
            state.last_remaining = int(response.headers['X-RateLimit-Remaining'])
        except KeyError:
            # This is the strange and rare case (which we've encountered) where
            # an API call that normally returns the rate-limit headers doesn't
            # return them. Since these values are only used for monitoring, we
            # use nonsensical values here that will show it happened, but won't
            # crash a long-running process.
            state.last_ratelimit = 999999
            state.last_remaining = 999999

        if state.verbose:
            # display rate-limite status
            username = auth[0] if auth else '(non-authenticated)'
            used = state.last_ratelimit - state.last_remaining
            print('  Rate Limit: {0} available, {1} used, {2} total for {3}'. \
                format(state.last_remaining, used, state.last_ratelimit, username))

    return response

def gzunzip(zippedfile, unzippedfile): #-------------------------------------<<<
    """Decompress a .gz (GNU Zip) file.
    """
    with open(unzippedfile, 'w') as fhandle:
        fhandle.write('githubuser,email\n')
        for line in gzip.open(zippedfile).readlines():
            jsondata = json.loads(line.decode('utf-8'))
            outline = jsondata['ghu'] + ',' + jsondata['aadupn']
            fhandle.write(outline + '\n')

def setting(topic, section, key): #------------------------------------------<<<
    """Retrieve a private setting stored in a local .ini file.

    topic = name of the ini file; e.g., 'azure' for azure.ini
    section = section within the .ini file
    key = name of the key within the section

    Returns the value if found, None otherwise.
    """
    source_folder = os.path.dirname(os.path.realpath(__file__))
    inifile = os.path.join(source_folder, '../_private/' + topic.lower() + '.ini')
    config = configparser.ConfigParser()
    config.read(inifile)
    try:
        retval = config.get(section, key)
    except configparser.NoSectionError:
        retval = None
    return retval
