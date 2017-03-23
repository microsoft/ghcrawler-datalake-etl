"""
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.

ghiverify.py - tools to verify data integrity for GHInsights data
"""
import collections
import configparser
import csv
import datetime
import glob
import gzip
import json
import os
import re
import sys
from timeit import default_timer

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.datalake.store import core, lib, multithread
import requests

#----------------------------------------------------------------------------<<<
# MISCELLANEOUS                                                              <<<
#----------------------------------------------------------------------------<<<

class _settings: #-----------------------------------------------------------<<<
    """This class exists to provide a namespace used for global settings.
    """
    requests_session = None # current session object from requests library
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

def daily_diff(): #----------------------------------------------------------<<<
    """Generate diff files for all tracked entities.
    """
    datestr = str(datetime.datetime.now())[:10]
    start_time = default_timer()
    print_log(10*'-' + ' Data Verification for ' + datestr + ' ' + 10*'-')

    for entity in ['repo']:
        entity_start = default_timer()

        # download the current CSV file from Azure Data Lake Store
        download_start = default_timer()
        datalake_download_entity(entity)
        print_log('Download ' + entity + '.csv from Data Lake '.ljust(27, '.') +
                  '{0:6.1f} seconds, {1:,} bytes'.format( \
            default_timer() - download_start, \
            filesize(datafile_local('repo', 'datalake'))))

        # get current results from GitHub API for this entity
        github_start = default_timer()
        github_get_entity(entity)
        print_log('Get live data from GitHub API '.ljust(40, '.') +
                  '{0:6.1f} seconds, {1:,} bytes'.format( \
            default_timer() - github_start, \
            filesize(datafile_local('repo', 'github'))))

        # generate diff report
        diff_start = default_timer()
        missing, extra, mismatch = diff_report(entity)
        print_log('Generate ' + entity + '_diff.csv '.ljust(27, '.') +
                  '{0:6.1f} seconds, {1:,} bytes'.format( \
            default_timer() - diff_start, \
            filesize('repo_diff.csv')))
        if missing:
            print_log(24*' ' + 'Missing: {:7,} '.format(missing) +
                      entity + 's')
        if extra:
            print_log(24*' ' + 'Extra:   {:7,} '.format(extra) +
                      entity + 's')
        if mismatch:
            print_log(24*' ' + 'Mismatch:{:7,} '.format(mismatch) +
                      entity + 's')

        # upload diff report CSV file to Azure Data Lake Store
        upload_start = default_timer()
        datalake_upload_entity(entity)
        print_log('Upload ' + entity + '_diff to Data Lake '.ljust(29, '.') +
                  '{0:6.1f} seconds'.format(default_timer() - upload_start))

        entity_elapsed = default_timer() - entity_start
        print_log(entity.upper().rjust(24) +
                  ' - elapsed time:{0:6.1f} seconds'.format(entity_elapsed))

    print(54*'-')
    print_log('    Total elapsed time for all entities:{:6.1f} seconds'. \
        format(default_timer() - start_time))

def datafile_local(entity=None, filetype=None): #----------------------------<<<
    """Returns relative path/filename for a local CSV file.

    Note that today's date is included in the filename:
    data/<entity>-<filetype>-YYYY-MM-DD.csv
    """
    datestr = str(datetime.datetime.now())[:10]
    return 'data/' + entity.lower() + '-' + filetype.lower() + '-' +\
        datestr + '.csv'

def date_range(datestr): #---------------------------------------------------<<<
    """Convert date string (YYYY-MM-DD) to one of the following date-range
    categories.
    """
    days_old = days_since(datestr)
    if days_old <= 30:
        return '030'
    elif days_old <= 60:
        return '060'
    elif days_old <= 90:
        return '090'
    elif days_old <= 180:
        return '180'
    elif days_old <= 365:
        return '365'
    return 'older'

def days_since(datestr): #---------------------------------------------------<<<
    """Return # days since a date in YYYY-MM-DD format.
    """
    return (datetime.datetime.today() -
            datetime.datetime.strptime(datestr, '%Y-%m-%d')).days

def diff_report(entity=None, masterfile=None, comparefile=None): #-----------<<<
    """Generate a diff report for specified entity.

    If only an entity is passed, today's github/datalake data files for this
    entity are used for the comparison, and diff results are written to
    <entity>_diff.csv.

    If masterfile/comparefile parameters are passed, those files are compared
    and diff results are written to diff_report.csv.

    returns (missing, extra, mismatch) tuple, or None if errors.
    """
    if entity and masterfile and comparefile:
        return #/// diff these two files, based on entity type specified
    if masterfile or comparefile or not entity:
        print('ERROR: invalid arguments passed to diff_report().')
        return

    if entity.lower() == 'repo':
        return repo_diff()
    else:
        print('ERROR: unknown diff_report() entity type = ' + entity)

def documentation_repo(reponame): #------------------------------------------<<<
    """Check repo name for whether it appears to be a documentation repo.
    """
    docrepos = [r'.*-pr\..{2}-.{2}.*', r'.*\..{2}-.{2}.*', r'.*-pr$',
                r'.*\.handoff.*', r'handback', r'ontent-{4}\/']
    for regex in docrepos:
        if re.match(regex, reponame):
            return True

    return False

def filesize(filename): #----------------------------------------------------<<<
    """Return byte size of specified file.
    """
    return os.stat(filename).st_size

def local_filename(entity, source): #----------------------------------------<<<
    """Return filename of a local data file.

    entity = entity type (e.g., 'repo')
    source = data source (e.g., 'datalake' or 'github')

    Returns the most recently captured local data filename for this combination
    of entity type and data source.
    """
    return max(glob.glob('data/' + entity.lower() + '-' + source.lower() +
                         '-*.csv'))

def microsoft_vp(alias): #---------------------------------------------------<<<
    """Return the alias of Satya's direct for a specified Microsoft alias.
    """
    if not hasattr(_settings, 'alias_manager'):
        # load dictionary first time this function is called
        _settings.alias_manager = dict()
        for line in open('data/aliasManager.csv', 'r').readlines():
            person, manager = line.strip().split(',')
            _settings.alias_manager[person.lower()] = manager.lower()

    # the approach here is to traverse up the management chain to Satya Nadella,
    # and return the "Satya's direct" for this employee. Note that we limit the
    # depth of this search to 20 levels of management (a horrific concept), to
    # avoid an infinite loop if there is bogus data that creates a circular
    # relationship (e.g., your manager reports to you).
    max_depth = 20
    current = alias.lower() # current person as we move up the mgmt chain
    while True:
        max_depth -= 1
        if max_depth < 1:
            return '*unknown*'
        # relationship in the data; need to implement a max-depth concept
        mgr = _settings.alias_manager[current]
        if mgr == 'satyan':
            break # current = satya's direct
        current = mgr # move up to the next manager

    return current

def ms_email(github_user): #-------------------------------------------------<<<
    """Return Microsoft email address linked to a GitHub account.
    """
    if not hasattr(_settings, 'linkdata'):
        # load dictionary first time this function is called
        _settings.linkdata = dict()
        myreader = csv.reader(open('data/linkdata.csv', 'r'), delimiter=',', quotechar='"')
        next(myreader, None)
        for values in myreader:
            _settings.linkdata[values[0].lower()] = values[1].lower()
    return _settings.linkdata.get(github_user.lower(), '')

def orgchart_shredder(): #---------------------------------------------------<<<
    """Shred most recent orgchart data file into CSVs used for lookups.
    """
    # get most recent orgchart data filename
    orgchartdata = max(glob.glob('data/organizationchart-datalake-*.csv'))

    outfile1 = open('data/aliasManager.csv', 'w')
    outfile2 = open('data/emailAlias.csv', 'w')
    # emails_written is the unique set of email addresses that have been
    # processed, and is used to avoid writing duplicate records to outfile2
    emails_written = set()
    count = 0
    myreader = csv.reader(open(orgchartdata, 'r', encoding='iso-8859-2'),
                          delimiter=',', quotechar='"')
    for values in myreader:
        count += 1
        alias = values[1].lower()
        #fullname = values[2]
        manager = values[3].lower()
        email1 = values[4].lower()
        email2 = values[5].lower()
        outfile1.write(alias + ',' + manager + '\n')
        if not email1 in emails_written:
            outfile2.write(email1 + ',' + alias + '\n')
            emails_written.add(email1)
        if not email2 in emails_written:
            outfile2.write(email2 + ',' + alias + '\n')
            emails_written.add(email2)

    outfile1.close()
    outfile2.close()
    print('{0} orgchart records processed.'.format(count))

def print_log(text): #-------------------------------------------------------<<<
    """Print a a line of text and add it to ghiverify.log log file.
    """
    print(text)
    with open('ghiverify.log', 'a') as fhandle:
        fhandle.write(str(datetime.datetime.now())[:22] + ' ' + text + '\n')

def privaterepos(): #--------------------------------------------------------<<<
    """Generate privateRepos.csv file.

    Currently generating this from GitHub API, next step is to make this a
    U-SQL job to generate CSV file from the ghinsightsms Azure Data Lake Store.
    """
    paid_orgs = ['azuread', 'azure', 'azure-samples', 'microsoft',
                 'aspnet', 'contosodev', 'contosotest']
    datafile = local_filename('repo', 'datalake')
    outfile = open('privateRepos.csv', 'w')
    outfile.write('org,repo,created,paid,age,inactive,days_old,' +
                  'days_inactive,last_activity,over30,doc_repo,' +
                  'last_push,last_update\n')
    rows_written = 0

    myreader = csv.reader(open(datafile, 'r', encoding='iso-8859-2'),
                          delimiter=',', quotechar='"')
    for values in myreader:
        private = values[60]
        if not private.lower() == 'true':
            continue # only include private repos

        repo = values[3]
        org = values[4]
        if org.lower() == 'msftberlin' or org.lower().startswith('6wunder'):
            continue # don't include known internal engineering groups

        created = values[5][:10]
        last_push = values[61][:10]
        last_update = values[93][:10]
        last_activity = max(created, last_push, last_update)
        paid = org.lower() in paid_orgs
        doc_repo = documentation_repo(repo)
        days_old = days_since(created)
        days_inactive = days_since(last_activity)
        over30 = days_old > 30
        age = date_range(created)
        inactive = date_range(last_activity)
        data = [org, repo, created, str(paid), '"' + age + '"',
                '"' + inactive + '"', str(days_old), str(days_inactive),
                last_activity, str(over30), str(doc_repo), last_push,
                last_update]
        outfile.write(','.join(data) + '\n')
        rows_written += 1

    outfile.close()
    print('{0} rows written to privateRepos.csv'.format(rows_written))

def setting(topic=None, section=None, key=None): #---------------------------<<<
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

def token_creds(): #---------------------------------------------------------<<<
    """Return token and credentials for Azure Active Directory authentication.
    """
    tenantid = setting(topic='ghiverify', section='aad', key='tenant-id')
    clientsecret = setting(topic='ghiverify', section='aad', key='client-secret')
    clientid = setting(topic='ghiverify', section='aad', key='client-id')
    return (
        lib.auth(tenant_id=tenantid,
                 client_secret=clientsecret,
                 client_id=clientid),
        ServicePrincipalCredentials(client_id=clientid,
                                    secret=clientsecret,
                                    tenant=tenantid))

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
# AZURE                                                                      <<<
#----------------------------------------------------------------------------<<<

def latestlinkdata(): #------------------------------------------------------<<<
    """Returns the most recent filename for Azure blobs that contain linkdata.
    """
    azure_acct = setting('azure','linkingdata', 'account')
    azure_key = setting('azure','linkingdata', 'key')
    azure_container = setting('azure','linkingdata', 'container')

    from azure.storage.blob import BlockBlobService
    block_blob_service = BlockBlobService(account_name=azure_acct, account_key=azure_key)
    blobs = block_blob_service.list_blobs(azure_container)
    latest = ''
    for blob in blobs:
        latest = blob.name if blob.name > latest else latest
    return latest if latest else None

def linkingdata_update(): #--------------------------------------------------<<<
    """Update local copy of GitHub-Microsoft account linking data.
    """
    azure_acct = setting('azure', 'linkingdata', 'account')
    azure_key = setting('azure', 'linkingdata', 'key')
    azure_container = setting('azure', 'linkingdata', 'container')
    azure_blobname = latestlinkdata()
    gzfile = 'data/' + azure_blobname
    print('retrieving link data: ' + azure_blobname)

    # download the Azure blob
    from azure.storage.blob import BlockBlobService
    block_blob_service = BlockBlobService(account_name=azure_acct, account_key=azure_key)
    block_blob_service.get_blob_to_path(azure_container, azure_blobname, gzfile)

    # decompress the JSON file and write to linkdata.csv
    outfile = 'data/linkdata.csv'
    with open(outfile, 'w') as fhandle:
        fhandle.write('githubuser,email\n')
        for line in gzip.open(gzfile).readlines():
            jsondata = json.loads(line.decode('utf-8'))
            outline = jsondata['ghu'] + ',' + jsondata['aadupn']
            fhandle.write(outline + '\n')

#----------------------------------------------------------------------------<<<
# DATA LAKE                                                                  <<<
#----------------------------------------------------------------------------<<<

def datalake_dir(folder, token=None): #--------------------------------------<<<
    """Get a directory for an ADL filesystem folder.

    Returns a list of filenames.
    """
    if not token:
        token, _ = token_creds()
    adls_account = setting(topic='ghiverify', section='azure', key='adls-account')
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_account)

    return sorted([filename.split('/')[1] for
                   filename in adls_fs_client.listdir(folder)],
                  key=lambda fname: fname.lower())

def datalake_download_entity(entity=None, token=None): #---------------------<<<
    """Download specified entity type's CSV file from ghinsights Data Lake store.

    entity = entity type (for example, 'repo')
    token = Oauth token for Azure Data Lake; default = token_creds()

    Downloads a CSV file from the ghinsightsms Azure Data Lake Store.
    """
    localfile = datafile_local(entity=entity, filetype='datalake')
    remotefile = datalake_filename(entity=entity)
    datalake_download_file(localfile, remotefile, token)

def datalake_download_file(localfile, remotefile, token=None): #-------------<<<
    """Download a file from Azure Data Lake Store.
    """
    if not token:
        token, _ = token_creds()

    adls_account = setting(topic='ghiverify', section='azure', key='adls-account')
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_account)

    multithread.ADLDownloader(adls_fs_client, lpath=localfile,
                              rpath=remotefile, nthreads=64, overwrite=True,
                              buffersize=4194304, blocksize=4194304)

def datalake_filename(entity=None): #----------------------------------------<<<
    """Return path/filename for the Azure Data Lake CSV file for
    specified entity type.

    Note that we assume title-case. This is correct for Repo.csv and others,
    but there are some exceptions to be addressed (or eliminated) in the current
    data on Data Lake.
    """
    return '/TabularSource2/' + entity + '.csv'

def datalake_list_accounts(): #----------------------------------------------<<<
    """List the available Azure Data Lake storage accounts.
    """
    _, credentials = token_creds()

    subscription_id = setting('ghiverify', 'azure', 'subscription')
    adls_acct_client = \
        DataLakeStoreAccountManagementClient(credentials, subscription_id)

    result_list_response = adls_acct_client.account.list()
    result_list = list(result_list_response)
    for items in result_list:
        print('--- Azure Data Lake Storage Account ---')
        print('Name:     ' + items.name)
        print('Endpoint: ' + items.endpoint)
        print('Location: ' + str(items.location))
        print('Created:  ' + str(items.creation_time.date()))
        print('ID:       ' + str(items.id))

def datalake_upload_entity(entity=None): #-----------------------------------<<<
    """Upload a specified entity's diff file to Azure Data Lake storage.
    """
    localfile = entity.lower() + '_diff.csv'
    remotefile = '/users/dmahugh/' + localfile
    datalake_upload_file(localfile, remotefile)

def datalake_upload_file(localfile, remotefile, token=None): #---------------<<<
    """Upload a file to an Azure Data Lake Store.

    Note that the remote filename should be relative to the root.
    For example: '/users/dmahugh/repo_diff.csv'
    """
    if not token:
        token, _ = token_creds()

    adls_account = setting(topic='ghiverify', section='azure', key='adls-account')
    adls_fs_client = \
        core.AzureDLFileSystem(token, store_name=adls_account)

    multithread.ADLUploader(adls_fs_client, lpath=localfile,
                            rpath=remotefile, nthreads=64, overwrite=True,
                            buffersize=4194304, blocksize=4194304)

#----------------------------------------------------------------------------<<<
# GITHUB                                                                     <<<
#----------------------------------------------------------------------------<<<

def github_api(*, endpoint=None): #------------------------------------------<<<
    """Call the GitHub API with default authentication credentials.

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

    #used = _settings.last_ratelimit - _settings.last_remaining
    #/// shouldn't happen often, but need to decide how to handle rate-limit issues
    #print('  Rate Limit: ' + str(_settings.last_remaining) + ' available, ' +
    #      str(used) + ' used, ' + str(_settings.last_ratelimit) + ' total ' +
    #      auth[0])

    return response

def github_commit_count(org, repo): #----------------------------------------<<<
    """Return total number of commits for specified org/repo.
    """
    endpoint = 'https://api.github.com/repos/' + org + '/' + repo + '/commits'
    requests_session = requests.session()

    # get first page of results
    firstpage = requests_session.get(endpoint,\
        headers={"Accept": "application/vnd.github.v3+json"})
    if not firstpage.ok:
        return str(firstpage) # 404 errors, etc.
    pagelinks = github_pagination(firstpage)
    json_first = json.loads(firstpage.text)
    pagesize = len(json_first) # of items on the first page of results
    totpages = int(pagelinks['lastpage'])
    lastpage_url = pagelinks['lastURL']

    if not lastpage_url:
        return pagesize # only one page of results, so we're done

    # get last page of results
    lastpage = requests_session.get(lastpage_url,\
        headers={"Accept": "application/vnd.github.v3+json"})
    json_last = json.loads(lastpage.text)
    lastpage_count = len(json_last) # number of items on the last page

    return (pagesize * (totpages - 1)) + lastpage_count

def github_data(*, endpoint=None, fields=None): #----------------------------<<<
    """Get data for specified GitHub API endpoint.

    endpoint     = HTTP endpoint for GitHub API call
    fields       = list of fields to be returned

    Returns a list of dictionaries containing the specified fields.
    Returns a complete data set - if this endpoint does pagination, all pages
    are retrieved and aggregated.
    """
    all_fields = github_data_from_api(endpoint=endpoint)

    # extract the requested fields and return them
    retval = []
    for json_item in all_fields:
        retval.append(github_fields(jsondata=json_item, fields=fields))
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

def github_fields(*, jsondata=None, fields=None): #--------------------------<<<
    """Get dictionary of desired values from GitHub API JSON payload.

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

def github_get_entity(entity=None): #----------------------------------------<<<
    """Get live data from GitHub API for a specified entity type.

    There are variations in how each entity type can be verified, so this
    function is essentially a dispatcher to call entity-specific functions.
    """
    if entity.lower() == 'repo':
        github_get_repos()
    else:
        print('ERROR: unknown github_get_entity() argument - ' + entity)

def github_get_repos(): #----------------------------------------------------<<<
    """Retrieve repo data from GitHub API and store as CSV file.
    """
    filename = datafile_local('repo', 'github')

    # get a list of the orgs that authname is a member of
    templist = github_data(endpoint='/user/orgs', fields=['login'])
    sortedlist = sorted([_['login'].lower() for _ in templist])
    user_orgs = [orgname for orgname in sortedlist
                 if not orgname == 'nuget']

    repolist = [] # the list of repos
    for orgid in user_orgs:
        endpoint = '/orgs/' + orgid + '/repos?per_page=100'
        repolist.extend(github_data(endpoint=endpoint, \
            fields=['owner.login', 'name', 'created_at']))

    sorted_data = sorted(repolist, key=data_sort)
    write_csv(sorted_data, filename) # write CSV file

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
    Returns a tuple of total missing, extra, mismatch.
    """

    # handle default filenames
    if not github or not datalake:
        github = datafile_local('repo', 'github')
        datalake = datafile_local('repo', 'datalake')

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

    # write output file
    with open('repo_diff.csv', 'w') as outfile:
        outfile.write('org,repo,issue\n')
        for org, repo in sorted(missing):
            outfile.write(','.join([org, repo, 'missing']) + '\n')
        for org, repo in sorted(extra):
            outfile.write(','.join([org, repo, 'extra']) + '\n')
        for org, repo in sorted(mismatch):
            outfile.write(','.join([org, repo, 'mismatch']) + '\n')

    return (len(missing), len(extra), len(mismatch))

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
    if documentation_repo(reponame):
        return False

    return not created_at == str(datetime.datetime.now())[:10]

#----------------------------------------------------------------------------<<<
# TESTS                                                                      <<<
#----------------------------------------------------------------------------<<<

def test_commit_count(): #---------------------------------------------------<<<
    """Test cases for github_commit_count()
    """
    testcases = ['microsoft/dotnet',
                 'microsoft/vscode',
                 'microsoft/typescript',
                 'microsoft/xaml-standard',
                 'microsoft/ospo-witness',
                 'microsoft/ghcrawler-datalake-etl']
    for orgrepo in testcases:
        orgname = orgrepo.split('/')[0]
        reponame = orgrepo.split('/')[1]
        commits = github_commit_count(orgname, reponame)
        print(orgrepo + ', total commits = {0}'.format(commits))

# code to be executed when running standalone (for ad-hoc testing, etc.)
if __name__ == '__main__':
    #daily_diff()
    #test_commit_count()
    #privaterepos()
    #linkingdata_update()
    #datalake_download_entity('OrganizationChart')

    orgchart_shredder()
