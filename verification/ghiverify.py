"""
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.

ghiverify.py - tools to verify data integrity for GHInsights data
"""
# Prerequisites: pip install these packages ...
# azure, azure-mgmt-resource, azure-mgmt-datalake-store, azure-datalake-store

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.mgmt.datalake.store.models import DataLakeStoreAccount
from azure.datalake.store import core, lib, multithread

import collections
import configparser
import csv
import datetime
import getpass
import json
import os
import re
import sys

import requests

#----------------------------------------------------------------------------<<<
# MISCELLANEOUS                                                              <<<
#----------------------------------------------------------------------------<<<

class _settings: #-----------------------------------------------------------<<<
    """This class exists to provide a namespace used for global settings.
    """
    requests_session = None # current session object from requests library
    tot_api_calls = 0 # number of API calls made
    tot_api_bytes = 0 # total bytes returned by these API calls
    last_ratelimit = 0 # API rate limit for the most recent API call
    last_remaining = 0 # remaining portion of rate limit after last API call

def data_sort(datadict): #---------------------------------------------------<<<
    """Sort function for output lists.

    takes an OrderedDict object as input, returns lower-case version of the
    first value in the OrderedDict, for use as a sort key.
    """
    sortkey = list(datadict.keys())[0]
    sortvalue = str(datadict[sortkey]).lower()
    return sortvalue

def setting(topic=None, section=None, key=None): #---------------------------<<<
    """Retrieve a private setting stored in a local .ini file.

    topic = name of the ini file; e.g., 'azure' for azure.ini
    section = section within the .ini file
    key = name of the key within the section

    Returns the value if found, None otherwise.
    """
    source_folder = os.path.dirname(os.path.realpath(__file__))
    datafile = os.path.join(source_folder, '../_private/' + topic.lower() + '.ini')
    config = configparser.ConfigParser()
    config.read(datafile)
    try:
        retval = config.get(section, key)
    except configparser.NoSectionError:
        retval = None
    return retval

def token_creds(): #---------------------------------------------------------<<<
    """Return token and credentials for Azure Active Directory authentication.
    """
    tenantId = setting(topic='ghiverify', section='aad', key='tenant-id')
    clientSecret = setting(topic='ghiverify', section='aad', key='client-secret')
    clientId = setting(topic='ghiverify', section='aad', key='client-id')
    return (
        lib.auth(tenant_id=tenantId,
                 client_secret=clientSecret,
                 client_id=clientId),
        ServicePrincipalCredentials(client_id=clientId,
                                    secret=clientSecret,
                                    tenant=tenantId))

def write_csv(listobj, filename): #------------------------------------------<<<
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

#----------------------------------------------------------------------------<<<
# AZURE DATA LAKE                                                            <<<
#----------------------------------------------------------------------------<<<

def datalake_dir(folder, token=None): #--------------------------------------<<<
    """Get a directory for an ADL filesystem folder.

    Returns a list of filenames.
    """
    if not token:
        token, _ = token_creds()
    adlsAccount = setting(topic='ghiverify', section='azure', key='adls-account')
    adlsFileSystemClient = \
        core.AzureDLFileSystem(token, store_name=adlsAccount)

    return sorted([filename.split('/')[1] for
                   filename in adlsFileSystemClient.listdir(folder)],
                  key=lambda fname: fname.lower())

def datalake_download(remotefile, localfile, token=None): #------------------<<<
    """Download a file from Azure Data Lake Store.

    Note that the remote filename should be relative to the root.
    For example: '/users/dmahugh/repo_diff.csv'
    """
    token, _ = token_creds()

    adlsAccount = setting(topic='ghiverify', section='azure', key='adls-account')
    adlsFileSystemClient = \
        core.AzureDLFileSystem(token, store_name=adlsAccount)

    multithread.ADLDownloader(adlsFileSystemClient, lpath=localfile,
                              rpath=remotefile, nthreads=64, overwrite=True,
                              buffersize=4194304, blocksize=4194304)

    print('Downloaded: ' + localfile)
    print(' File size: {0:,} bytes'.format(os.stat(localfile).st_size))

def datalake_get_repos(): #--------------------------------------------------<<<
    """Retrieve the current Repo.csv data file from Azure Data Lake Store.
    """
    file_remote = '/TabularSource2/Repo.csv'
    datestr = str(datetime.datetime.now())[:10]
    file_local = 'data/repo-datalake-' + datestr + '.csv'
    datalake_download(file_remote, file_local)

def datalake_list_accounts(dlsaMgmtClient=None): #---------------------------<<<
    """List the available Azure Data Lake storage accounts.
    """
    token, credentials = token_creds()

    subscriptionId = setting(topic='ghiverify', section='azure', key='subscription')
    adlsAcctClient = \
        DataLakeStoreAccountManagementClient(credentials, subscriptionId)

    result_list_response = adlsAcctClient.account.list()
    result_list = list(result_list_response)
    for items in result_list:
        print('--- Azure Data Lake Storage Account ---')
        print('Name:     ' + items.name)
        print('Endpoint: ' + items.endpoint)
        print('Location: ' + str(items.location))
        print('Created:  ' + str(items.creation_time.date()))
        print('ID:       ' + str(items.id))

def datalake_upload(localfile, remotefile, token=None): #--------------------<<<
    """Upload a file to an Azure Data Lake Store.

    Note that the remote filename should be relative to the root.
    For example: '/users/dmahugh/repo_diff.csv'
    """
    if not token:
        token, _ = aadauth()

    adlsAccount = setting(topic='ghiverify', section='azure', key='adls-account')
    adlsFileSystemClient = \
        core.AzureDLFileSystem(token, store_name=adlsAccount)

    multithread.ADLUploader(adlsFileSystemClient, lpath=localfile,
                            rpath=remotefile, nthreads=64, overwrite=True,
                            buffersize=4194304, blocksize=4194304)

    print('Uploaded:  ' + localfile)
    print('File size: {0:,} bytes'.format(os.stat(localfile).st_size))

#----------------------------------------------------------------------------<<<
# GITHUB                                                                     <<<
#----------------------------------------------------------------------------<<<

def github_api(*, endpoint=None): #------------------------------------------<<<
    """Call the GitHub API.

    endpoint     = the HTTP endpoint to call; if it start with /, will be
                   appended to https://api.github.com

    Returns the response object.

    NOTE: sends the Accept header to use version V3 of the GitHub API. This can
    be explicitly overridden by passing a different Accept header if desired.
    """

    auth = (setting(topic='ghiverify', section='github', key='username'),
            setting(topic='ghiverify', section='github', key='pat'))

    # pass the GitHub API V3 Accept header
    headers = {"Accept": "application/vnd.github.v3+json"}

    # make the API call
    if _settings.requests_session:
        sess = _settings.requests_session
    else:
        sess = requests.session()
        _settings.requests_session = sess

    sess.auth = auth
    full_endpoint = 'https://api.github.com' + endpoint if endpoint[0] == '/' \
        else endpoint
    response = sess.get(full_endpoint, headers=headers)

    print('    Endpoint: ' + endpoint)

    # update rate-limit settings
    try:
        _settings.last_ratelimit = int(response.headers['X-RateLimit-Limit'])
        _settings.last_remaining = int(response.headers['X-RateLimit-Remaining'])
    except KeyError:
        # This is the strange and rare case (which we've encountered) where
        # an API call that normally returns the rate-limit headers doesn't
        # return them. Since these values are only used for monitoring, we
        # use nonsensical values here that will show it happened, but won't
        # crash a long-running process.
        _settings.last_ratelimit = 999999
        _settings.last_remaining = 999999

    used = _settings.last_ratelimit - _settings.last_remaining
    print('  Rate Limit: ' + str(_settings.last_remaining) + ' available, ' +
          str(used) + ' used, ' + str(_settings.last_ratelimit) + ' total ' +
          auth[0])

    return response

def github_data(*, endpoint=None, entity=None, fields=None): #---------------<<<
    """Get data for specified GitHub API endpoint.

    endpoint     = HTTP endpoint for GitHub API call
    entity       = entity type ('repo', 'member')
    fields       = list of fields to be returned

    Returns a list of dictionaries containing the specified fields.
    Returns a complete data set - if this endpoint does pagination, all pages
    are retrieved and aggregated.
    """
    all_fields = github_data_from_api(endpoint=endpoint)

    # extract the requested fields and return them
    retval = []
    for json_item in all_fields:
        retval.append(github_fields(entity=entity, jsondata=json_item,
                                  fields=fields))
    return retval

def github_data_from_api(endpoint=None): #-----------------------------------<<<
    """Get data from GitHub REST API.

    endpoint     = HTTP endpoint for GitHub API call

    Returns the data as a list of dictionaries. Pagination is handled by this
    function, so the complete data set is returned.
    """
    payload = [] # the full data set (all fields, all pages)
    page_endpoint = endpoint # endpoint of each page in the loop below

    while True:
        response = github_api(endpoint=page_endpoint)
        if response.ok:
            thispage = json.loads(response.text)
            # commit data is handled differently from everything else, because
            # the sheer volume (e.g., over 100K commits in a repo) causes out of
            # memory errors if all fields are returned.
            if 'commit' in endpoint:
                minimized = [_['commit'] for _ in thispage]
                payload.extend(minimized)
            else:
                payload.extend(thispage)
        else:
            print('ERROR: bad response from {0}, status = {1}'.format(endpoint, str(response)))


        pagelinks = github_pagination(response)
        page_endpoint = pagelinks['nextURL']
        if not page_endpoint:
            break # no more results to process

    return payload

def github_fields(*, entity=None, jsondata=None, fields=None): #-------------<<<
    """Get dictionary of desired values from GitHub API JSON payload.

    entity   = entity type ('repo', 'member')
    jsondata = a JSON payload returned by the GitHub API
    fields   = list of names of fields (entries) to include from the JSON data,
               or one of these shorthand values:
               '*' -------> return all fields returned by GitHub API
               'nourls' --> return all non-URL fields (not *_url or url)
               'urls' ----> return all URL fields (*_url and url)

    Returns a dictionary of fieldnames/values.
    """
    values = collections.OrderedDict()

    for fldname in fields:
        if '.' in fldname:
            # special case - embedded field within a JSON object
            try:
                values[fldname.replace('.', '_')] = \
                    jsondata[fldname.split('.')[0]][fldname.split('.')[1]]
            except (TypeError, KeyError):
                values[fldname.replace('.', '_')] = None
        else:
            # simple case: copy a field/value pair
            try:
                values[fldname] = jsondata[fldname]
                if fldname.lower() == 'private':
                    values[fldname] = 'private' if jsondata[fldname] else 'public'
            except KeyError:
                pass # ignore unknown fields

    return values

def github_get_repos(): #----------------------------------------------------<<<
    """Retrieve repo data from GitHub API and store as CSV file.
    """
    datestr = str(datetime.datetime.now())[:10]
    filename = 'data/repo-github-' + datestr + '.csv'
    authname = setting(topic='ghiverify', section='github', key='username')

    # get a list of the orgs that authname is a member of
    templist = github_data(endpoint='/user/orgs', entity='org',
                           fields=['login'])
    sortedlist = sorted([_['login'].lower() for _ in templist])
    # note that we don't include contoso* orgs
    user_orgs = [orgname for orgname in sortedlist
                 if not orgname.startswith('contoso')]

    repolist = [] # the list of repos
    for orgid in user_orgs:
        endpoint = '/orgs/' + orgid + '/repos?per_page=100'
        repolist.extend(github_data(endpoint=endpoint, entity='repo', \
            fields=['owner.login', 'name', 'created_at']))

    sorted_data = sorted(repolist, key=data_sort)
    write_csv(sorted_data, filename) # write CSV file

    print('Output file written: ' + filename)
    print('          File size: {0:,} bytes'.format(os.stat(filename).st_size))

def github_pagination(link_header): #----------------------------------------<<<
    """Parse values from the 'link' HTTP header returned by GitHub API.

    1st parameter = either of these options ...
                    - 'link' HTTP header passed as a string
                    - response object returned by requests library

    Returns a dictionary with entries for the URLs and page numbers parsed
    from the link string: firstURL, firstpage, prevURL, prevpage, nextURL,
    nextpage, lastURL, lastpage.
    <internal>
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

#----------------------------------------------------------------------------<<<
# DIFFING REPOS                                                              <<<
#----------------------------------------------------------------------------<<<

def repo_data(filename): #---------------------------------------------------<<<
    """Load a repo CSV file into a list of tuples.

    Each tuple = (orgname, reponame, created)

    This function handles variations in file structure and returns a clean
    and consistent data set for comparison. All values are lower-case, and
    timestamp is trimmed to YYYY-MM-DD.
    """
    dataset = []
    encoding_type = 'ISO-8859-2' if 'datalake' in filename else 'UTF-8'
    for line in open(filename, 'r', encoding=encoding_type).readlines():
        if line.strip().lower() == 'owner_login,name,created_at':
            continue # skip header in GitHub data file

        values = line.strip().split(',')

        if 'datalake' in filename:
            org = values[4].strip('"').upper()
            repo = values[3].strip('"').lower()
            created_at = values[5][:10]
        else:
            org = values[0].upper()
            repo = values[1].lower()
            created_at = values[2][:10]

        if repo_include(repo, created_at):
            dataset.append((org, repo, created_at))

    return dataset

def repo_diff(github=None, datalake=None): #---------------------------------<<<
    """Diff two repo.csv files

    github = CSV file of repo data from the GitHub API (master copy)
    datalake = CSV file from ghinsightsms Azure Data Lake Store

    If filenames are not provided, defaults to today's files.

    Differences are displayed and also written to repo_diff.csv report file.
    """

    # handle default filenames
    if not github or not datalake:
        datestr = str(datetime.datetime.now())[:10]
        github = 'data/repo-github-' + datestr + '.csv'
        datalake = 'data/repo-datalake-' + datestr + '.csv'
        if not os.path.isfile(github):
            print('MISSING FILE: ' + github)
            sys.exit()
        if not os.path.isfile(datalake):
            print('MISSING FILE: ' + datalake)
            sys.exit()

    print('GitHub API data file: ' + github)
    print('Data Lake data file:  ' + datalake)

    print('loading data ...')
    repos_github = repo_data(github)
    repos_datalake = repo_data(datalake)

    missing = [] # repos missing from Data Lake
    extra = [] # repos in Data Lake but not GitHub
    mismatch = [] # repos with different creation dates

    # check for missing from Data Lake, or different created date
    for org, repo, created in repos_github:
        created_dl = repo_found(repos_datalake, org, repo)
        if created_dl:
            # this org/repo is in both files
            if not created == created_dl:
                mismatch.append((org, repo))
        else:
            missing.append((org, repo))

    # check for extra in Data Lake
    for org, repo, created in repos_datalake:
        if repo_found(repos_github, org, repo):
            pass # this org/repo is in both files
        else:
            extra.append((org, repo))

    print('writing output file ...')
    with open('repo_diff.csv', 'w') as outfile:
        outfile.write('org,repo,issue\n')
        for org, repo in sorted(missing):
            outfile.write(','.join([org, repo, 'missing']) + '\n')
        for org, repo in sorted(extra):
            outfile.write(','.join([org, repo, 'extra']) + '\n')
        for org, repo in sorted(mismatch):
            outfile.write(','.join([org, repo, 'mismatch']) + '\n')

    print('Total missing:  {0}'.format(len(missing)))
    print('Total extra:    {0}'.format(len(extra)))
    print('Total mismatch: {0}'.format(len(mismatch)))

def repo_found(dataset, org, repo): #----------------------------------------<<<
    """Check whether a dataset contains an org/repo.
    """
    for orgname, reponame, createddate in dataset:
        if orgname == org and reponame == repo:
            return createddate
    return False

def repo_include(reponame, created_at): #------------------------------------<<<
    """Check repo name for whether it's one we want to include.

    Certain types of repo names are excluded based on regex expressions.
    We also don't include repos created today.
    """
    if re.match(r'.*-pr\..{2}-.{2}.*', reponame):
        return False
    elif re.search(r'.*\..{2}-.{2}.*', reponame):
        return False
    elif re.match(r'.*-pr$', reponame):
        return False
    elif re.match(r'.*\.handoff.*', reponame):
        return False
    elif re.match(r'handback', reponame):
        return False
    elif re.match(r'ontent-{4}\/', reponame):
        return False
    return not created_at == str(datetime.datetime.now())[:10]

# code to be executed when running standalone (for ad-hoc testing, etc.)
if __name__ == '__main__':
    #datalake_get_repos()
    github_get_repos()
    #repo_diff()
    #datalake_list_accounts()
    #ghinsightsms_upload('repo_diff.csv', '/users/dmahugh/repo_diff.csv')
    #for file in datalake_dir('/TabularSource2/'):
    #    print(file)
