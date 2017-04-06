"""ghiverify.py - tools to verify data integrity for GHInsights data

Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.
"""
import collections
import csv
import datetime
import glob
import json
import re
import sys
from timeit import default_timer

import requests

from dougerino import days_since, dicts2csv, filesize, gzunzip
from dougerino import github_allpages, github_pagination, setting

from azurehelpers import azure_datalake_token, azure_blob_get, azure_blob_dir
from azurehelpers import datalake_put_file, datalake_get_file

#----------------------------------------------------------------------------<<<
# MISCELLANEOUS                                                              <<<
#----------------------------------------------------------------------------<<<

class _settings: #-----------------------------------------------------------<<<
    """This class exists to provide a namespace used for global settings.
    """
    requests_session = None # current session object from requests library
    last_ratelimit = 0 # API rate limit for the most recent API call
    last_remaining = 0 # remaining portion of rate limit after last API call

def add_execvp(): #----------------------------------------------------------<<<
    """Add execvp column to privateRepos.csv, write output as privateRepos2.csv.
    Note that this assumes the data/repoExecVP.csv data is current.
    """
    outfile = open('privateRepos2.csv', 'w')
    myreader = csv.reader(open('privateRepos.csv', 'r'),
                          delimiter=',', quotechar='"')
    header = next(myreader, None)
    outfile.write(','.join(header) + ',execvp\n')
    for values in myreader:
        org = values[0]
        repo = values[1]
        execvp = repo_execvp(org, repo)
        outfile.write(','.join(values) + ',' + execvp + '\n')

    outfile.close()

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

    for entity in ['Repo']:
        entity_start = default_timer()

        # download the current CSV file from Azure Data Lake Store
        download_start = default_timer()
        token, _ = azure_datalake_token('ghiverify')
        datalake_download_entity(entity, token)
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

def datalake_download_entity(entity, token): #-------------------------------<<<
    """Download specified entity's CSV file from ghinsights Data Lake store.

    entity = entity type (for example, 'repo')
    token = Oauth token for Azure Data Lake storage account

    Downloads a CSV file from the ghinsightsms Azure Data Lake Store.
    """
    localfile = datafile_local(entity=entity, filetype='datalake')
    remotefile = datalake_filename(entity=entity)
    adls_account = setting('ghiverify', 'azure', 'adls-account')
    datalake_get_file(localfile, remotefile, adls_account, token)

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
    _, credentials = azure_datalake_token('ghiverify')

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
    token, _ = azure_datalake_token('ghiverify')
    adls_account = setting('ghiverify', 'azure', 'adls-account')
    datalake_put_file(localfile, remotefile, adls_account, token)

def datalake_verification_download(): #--------------------------------------<<<
    """Download the verification data (daily totals for various entities).
    """
    datestr = str(datetime.datetime.now())[:10]
    remotefile = '/TabularSource2/verification_activities.csv'
    localfile = 'data/verification-datalake-' + datestr + '.csv'
    token, _ = azure_datalake_token('ghiverify')
    adls_account = setting('ghiverify', 'azure', 'adls-account')
    datalake_get_file(localfile, remotefile, adls_account, token)

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
        return # TO DO: diff these two files, based on entity type specified
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
    if '-pr_' in reponame.lower() or '-pr.' in reponame.lower():
        return True # a hack to exclude a bunch of Microsoft repos
    docrepos = [r'.*-pr\..{2}-.{2}.*', r'.*\..{2}-.{2}.*', r'.*-pr$',
                r'.*\.handoff.*', r'.*\.handback', r'ontent-{4}\/']
    for regex in docrepos:
        if re.match(regex, reponame):
            return True

    return False

def endpoint_for_pageno(endpoint, pageno): #---------------------------------<<<
    """Return the endpoint for a specified page # of the paginated result set,
    based on a well-formed endpoint for a (any) page and a desired pageno."""
    root, _ = endpoint.split('&')
    return root + '&page=' + str(pageno)

def endpoint_to_pageno(endpoint): #------------------------------------------<<<
    """Determine page # within a paginated result set, based on the endpoint
    that was hit to return the current page."""
    parms = endpoint.split('&')
    if len(parms) < 2:
        return 1
    return int(parms[-1].split('=')[-1])

def latestlinkdata(): #------------------------------------------------------<<<
    """Returns the most recent filename for Azure blobs that contain linkdata.
    """
    azure_acct = setting('azure', 'linkingdata', 'account')
    azure_key = setting('azure', 'linkingdata', 'key')
    azure_container = setting('azure', 'linkingdata', 'container')

    blobs = azure_blob_dir(azure_acct, azure_key, azure_container)

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

    azure_blob_get(azure_acct, azure_key, azure_container, azure_blobname, gzfile)
    gzunzip(gzfile, 'data/linkdata.csv')

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
    while current:
        max_depth -= 1
        if max_depth < 1:
            return '*unknown*'
        # relationship in the data; need to implement a max-depth concept
        mgr = _settings.alias_manager[current]
        if mgr == 'satyan':
            break # current = satya's direct
        current = mgr # move up to the next manager

    return current

def ms_alias(email): #-------------------------------------------------------<<<
    """Return Microsoft alias for an email address.
    """
    if not hasattr(_settings, 'email_alias'):
        # load dictionary first time this function is called
        _settings.email_alias = dict()
        myreader = csv.reader(open('data/emailAlias.csv', 'r'), delimiter=',', quotechar='"')
        next(myreader, None)
        for values in myreader:
            if values[0]:
                _settings.email_alias[values[0]] = values[1]
    return _settings.email_alias.get(email.lower(), '')

def ms_email(github_user): #-------------------------------------------------<<<
    """Return Microsoft email address linked to a GitHub account.
    """
    if not hasattr(_settings, 'linkdata'):
        # load dictionary first time this function is called
        _settings.linkdata = dict()
        myreader = csv.reader(open('data/linkdata.csv', 'r'), delimiter=',', quotechar='"')
        next(myreader, None)
        for values in myreader:
            if values[0]:
                _settings.linkdata[values[0]] = values[1]
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

def private_repo_admins(): #-------------------------------------------------<<<
    """Write data file of all admin users for private repos.

    Note this is hard-coded to working with ./privateRepos.csv, should make
    this more re-usable later.
    """
    outfile = 'data/privateRepoAdmins.csv'
    open(outfile, 'w').write('org,repo,githubuser,ms_email,ms_alias,exec\n')
    myreader = csv.reader(open('privateRepos.csv', 'r'),
                          delimiter=',', quotechar='"')
    next(myreader, None)

    for values in myreader:
        org = values[0]
        repo = values[1]
        print(org, repo)
        adminlist = repo_admins(org, repo)
        for githubuser in adminlist:
            email = ms_email(githubuser)
            alias = ms_alias(email)
            execvp = microsoft_vp(alias)
            if execvp: # only write a record if we an execvp for this user
                outdata = [org, repo, githubuser, email, alias, execvp]
                open(outfile, 'a').write(','.join(outdata) + '\n')

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

def repo_admins(org, repo): #------------------------------------------------<<<
    """Return set of GitHub usernames that have admin rights to specified repo.
    """
    admins = set() # lower-case GitHub usernames for all repo admins

    # get members of teams that have admin access
    teamdata = github_allpages('/repos/' + org + '/' + repo + '/teams')
    for team in teamdata:
        if team['permission'] == 'admin':
            members = github_allpages('/teams/' + str(team['id']) + '/members')
            for member in members:
                admins.add(member['login'].lower())

    # get external contributors
    contributors = github_allpages('/repos/' + org + '/' + repo + '/contributors')
    for contributor in contributors:
        admins.add(contributor['login'])

    # get org owners/admins
    orgowners = github_allpages('/orgs/' + org + '/members?role=admin')
    for owner in orgowners:
        admins.add(owner['login'])

    return sorted(admins)

def repo_execvp(orgname, reponame): #----------------------------------------<<<
    """Return the execvp for specified repo.
    """
    if not hasattr(_settings, 'repo_exec'):
        # load dictionary first time this function is called
        _settings.repo_exec = dict()
        myreader = csv.reader(open('data/repoExecVP.csv', 'r'), delimiter=',', quotechar='"')
        next(myreader, None)
        for values in myreader:
            if values[0] and values[1] and values[2]:
                org_repo = values[0].lower() + '/' + values[1].lower()
                _settings.repo_exec[org_repo] = values[2]
    return _settings.repo_exec.get(orgname.lower() + '/' + reponame.lower(), '')

def repo_execvp_voting(): #--------------------------------------------------<<<
    """Count the "votes" to determine which exec VP (Satya's directs) "owns"
    each private repo.

    Note this is hard-coded to specific filenames. Make this more generic.
    """
    infile = 'data/privateRepoAdmins.csv' # assumed to be sorted on org/repo

    # the votes data structure is an OrderedDict (in org/repo order) that
    # contains a dictionary of voting totals for each repo
    votes = collections.OrderedDict()
    myreader = csv.reader(open(infile, 'r'), delimiter=',', quotechar='"')
    next(myreader, None)
    for values in myreader:
        org_repo = values[0] + '/' + values[1]
        execvp = values[5]
        if org_repo in votes:
            if execvp in votes[org_repo]:
                votes[org_repo][execvp] += 1
            else:
                votes[org_repo][execvp] = 1
        else:
            votes[org_repo] = dict()
            votes[org_repo][execvp] = 1

    # write output file
    outfile = 'data/repoExecVP.csv'
    open(outfile, 'w').write('org,repo,execvp\n`')
    for org_repo in votes:
        execvp = max(votes[org_repo], key=votes[org_repo].get)
        open(outfile, 'a').write(org_repo.replace('/', ',') +
                                 ',' + execvp + '\n')

def repototal_commits(orgname, reponame): #----------------------------------<<<
    """Get a total # commits from a repo totals data file created by
    repototals_asofdate(). (Fields: org/repo, issues, pullrequests, commits.)"""
    if not hasattr(_settings, 'repo_tot_commits'):
        # load dictionary first time this function is called
        _settings.repo_tot_commits = dict()
        for line in open('repototals-2017-04-03.csv', 'r').readlines():
            orgrepo, _, _, commits = line.strip().split(',')
            _settings.repo_tot_commits[orgrepo.lower()] = int(commits)
    return _settings.repo_tot_commits.get( \
        orgname.lower() + '/' + reponame.lower(), 0)

def repototals_asofdate(rawdata, totfile, asofdate): #-----------------------<<<
    """Generate a file with total issues, pullrequests, commits for each repo
    as of specified date."""
    tot_issues = dict()
    tot_prs = dict()
    tot_commits = dict()
    myreader = csv.reader(open(rawdata, 'r'), delimiter=',', quotechar='"')
    for values in myreader:
        thedate = values[0]
        if thedate > asofdate:
            continue
        orgrepo = values[1]
        issues = int(values[2])
        prs = int(values[3])
        commits = int(values[4])
        if orgrepo in tot_issues:
            tot_issues[orgrepo] += issues
        else:
            tot_issues[orgrepo] = issues
        if orgrepo in tot_prs:
            tot_prs[orgrepo] += prs
        else:
            tot_prs[orgrepo] = prs
        if orgrepo in tot_commits:
            tot_commits[orgrepo] += commits
        else:
            tot_commits[orgrepo] = commits
    with open(totfile, 'w') as fhandle:
        for orgrepo in tot_issues:
            fhandle.write(','.join([orgrepo, str(tot_issues[orgrepo]), \
                str(tot_prs[orgrepo]), str(tot_commits[orgrepo])]) + '\n')

#----------------------------------------------------------------------------<<<
# GITHUB                                                                     <<<
#----------------------------------------------------------------------------<<<

def commits_asofdate(org, repo, asofdate): #---------------------------------<<<
    """Return cumulative # of commits for an org/repo as of a date.

    This is an optimized approach that is based on the assumption that there
    are relatively few commits after asofdate. Performance should be good for
    recent asofdate values.
    """
    requests_session = requests.session()
    requests_session.auth = (setting('ghiverify', 'github', 'username'),
                             setting('ghiverify', 'github', 'pat'))
    v3api = {"Accept": "application/vnd.github.v3+json"}

    # handle first page
    endpoint = 'https://api.github.com/repos/' + org + '/' + repo + \
        '/commits?per_page=100&page=1'
    firstpage = requests_session.get(endpoint, headers=v3api)
    pagelinks = github_pagination(firstpage)
    totpages = int(pagelinks['lastpage'])
    lastpage_url = pagelinks['lastURL']
    jsondata = json.loads(firstpage.text)
    if 'git repository is empty' in str(jsondata).lower() or \
        'not found' in str(jsondata).lower():
        return 0
    #print(str(jsondata)) #///
    commits_firstpage = len([commit for commit in jsondata \
        if commit['commit']['committer']['date'][:10] <= asofdate])

    if not lastpage_url:
        # just one page of results for this repo
        return commits_firstpage

    # handle last page
    lastpage = requests_session.get(lastpage_url, headers=v3api)
    commits_lastpage = len([commit for commit in json.loads(lastpage.text) \
        if commit['commit']['committer']['date'][:10] <= asofdate])
    if not commits_lastpage:
        return 0 # there are no commits before asofdate for this repo

    # scan back from first page to find start of the desired date range
    pageno = 1
    while jsondata[-1]['commit']['committer']['date'][:10] > asofdate:
        pageno += 1
        endpoint = endpoint_for_pageno(endpoint, pageno)
        thispage = requests_session.get(endpoint, headers=v3api)
        jsondata = json.loads(thispage.text)
        commits_firstpage = len([commit for commit in jsondata \
            if commit['commit']['committer']['date'][:10] <= asofdate])

    return (totpages - pageno - 1) * 100 + commits_firstpage + commits_lastpage

def commit_count_date(org, repo, thedate, datasource): #---------------------<<<
    """Returns commit count for a specified date.

    datasource = 'g' for GitHub API, 'c' for local CSV file (from ADLS)
    """
    if 'c' in datasource.lower():
        retval = 0
        datafile = 'verification_activities_repo.csv'
        myreader = csv.reader(open(datafile, 'r'), delimiter=',', quotechar='"')
        for values in myreader:
            if values[1] == 'unknown':
                continue #/// what do these mean?
            this_date = values[0]
            this_org, this_repo = values[1].lower().split('/')
            issues = values[2]
            pullrequests = values[3]
            commits = values[4]
            if thedate == this_date and this_org == org.lower() and this_repo == repo.lower():
                retval = int(commits)
                break
    else:
        retval = commit_count_date_github(org, repo, thedate)
    return retval

def commit_count_date_github(org, repo, thedate): #--------------------------<<<
    """Returns commit count for a specified date, from the GitHub API.
    """
    requests_session = requests.session()
    requests_session.auth = (setting('ghiverify', 'github', 'username'),
                             setting('ghiverify', 'github', 'pat'))
    v3api = {"Accept": "application/vnd.github.v3+json"}

    commits = 0 # total commits found for this date
    endpoint = 'https://api.github.com/repos/' + org + '/' + repo + '/commits?per_page=100&page=1'
    thispage = requests_session.get(endpoint, headers=v3api)
    totpages = int(github_pagination(thispage)['lastpage'])
    pages_checked = [] # list of pages visited so far
    # these are confusing because commits are returned in reverse date order ...
    page_before = 0 # the highest page# known to fall before thedate
    page_after = totpages + 1 # the lowest page# known to fall after thedate
    while True:
        if not thispage.ok:
            print('ERROR - {0} - endpoint: {1}'.format(str(thispage, endpoint)))
            break

        pageno = endpoint_to_pageno(endpoint)
        #print('page scanned (out of {0} total): {1}'.format(totpages, pages_checked))
        #print('bounded by page {0} to {1}, current page = {2}, totpages = {3}'. \
        #    format(page_before, page_after, pageno, totpages))
        pageno = endpoint_to_pageno(endpoint)
        if pageno in pages_checked:
            break # we've been here already, so we're done
        pages_checked.append(pageno)

        #print('pages visited: ' + str(pages_checked)) #///

        pagelinks = github_pagination(thispage)
        jsondata = json.loads(thispage.text)

        # determine the date range within this page
        highest_date = jsondata[0]['commit']['committer']['date'][:10]
        lowest_date = jsondata[-1]['commit']['committer']['date'][:10]

        if highest_date < thedate:
            # this page is entirely after (in page order) the desired date
            if pageno == 1:
                # nothing to do, we're looking for a date more recent than page 1
                break
            page_after = min(pageno, page_after)
            newpage = int((pageno + page_before)/2)
            endpoint = endpoint_for_pageno(endpoint, newpage)
            thispage = requests_session.get(endpoint, headers=v3api)
            continue
        if lowest_date > thedate:
            # this page is entirely before (in page order) the desired date
            if pageno == totpages:
                # nothing to do, we're looking for a date older than the last page
                break
            page_before = max(pageno, page_before)
            newpage = int((pageno + page_after)/2)
            endpoint = endpoint_for_pageno(endpoint, newpage)
            thispage = requests_session.get(endpoint, headers=v3api)
            continue

        # count commits on this page matching thedate
        hits = [True for commit in jsondata
                if commit['commit']['committer']['date'][:10] == thedate]
        commits += len(hits)

        if highest_date > thedate and lowest_date < thedate:
            break # all thedate commits were on this page, we're done

        if highest_date == thedate:
            if pageno <= 1:
                break
            endpoint = endpoint_for_pageno(endpoint, pageno - 1)
            thispage = requests_session.get(endpoint, headers=v3api)
            continue
        if lowest_date == thedate:
            if pageno >= totpages:
                break
            endpoint = endpoint_for_pageno(endpoint, pageno + 1)
            thispage = requests_session.get(endpoint, headers=v3api)

    return commits

def commit_count_sincedate(org, repo, sincedate=None): #---------------------<<<
    """Return total number of commits for specified org/repo.

    sincedate = optional; if passed, we only count commits for which the
                commit.committer.date is on or after sincedate
                format = string ('2017-04-01')

    If the optional sincedate argument is passed, returns the total number of
    commits on or after that date.
    """
    if not sincedate:
        sincedate = '1900-01-01'

    endpoint = 'https://api.github.com/repos/' + org + '/' + repo + '/commits'
    requests_session = requests.session()
    requests_session.auth = (setting('ghiverify', 'github', 'username'),
                             setting('ghiverify', 'github', 'pat'))

    # get first page of results
    firstpage = requests_session.get(endpoint,\
        headers={"Accept": "application/vnd.github.v3+json"})
    if not firstpage.ok:
        return str(firstpage) # 404 errors, etc.
    pagelinks = github_pagination(firstpage)
    json_first = json.loads(firstpage.text)
    most_recent = json_first[0]['commit']['committer']['date'][:10]
    if most_recent < sincedate:
        return 0 # there are no commits after sincedate
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
    first_commit = json_last[-1]['commit']['committer']['date'][:10]
    if sincedate <= first_commit:
        # first commit is since sincedate, so return total # commits
        return (pagesize * (totpages - 1)) + lastpage_count

    # edge cases are handled, so need to do binary search to find the count ...

    #/// commits_startin(jsondata, sincedate) - returns whether a sincedate STARTS
    # in a page (i.e., the page also includes an earlier date, OR the next page
    # includes ONLY previous dates)

    #/// commits_startpage(org, repo, sincedate) - returns which page # includes
    # sincedate (by splitting the pages recursively and calling commits_startin)

    #/// once we know which page to count FROM, do the math to count since that date

    return 123456 #///placeholder

def github_data(*, endpoint=None, fields=None): #----------------------------<<<
    """Get data for specified GitHub API endpoint.

    endpoint     = HTTP endpoint for GitHub API call
    fields       = list of fields to be returned

    Returns a list of dictionaries containing the specified fields.
    Returns a complete data set - if this endpoint does pagination, all pages
    are retrieved and aggregated.
    """
    all_fields = github_allpages(endpoint=endpoint)

    # extract the requested fields and return them
    retval = []
    for json_item in all_fields:
        retval.append(github_fields(jsondata=json_item, fields=fields))
    return retval

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
                 if not orgname in ['deployr', 'msftberlin', 'nuget']]

    repolist = [] # the list of repos
    for orgid in user_orgs:
        endpoint = '/orgs/' + orgid + '/repos?per_page=100'
        repolist.extend(github_data(endpoint=endpoint, \
            fields=['owner.login', 'name', 'created_at']))

    sorted_data = sorted(repolist, key=data_sort)
    dicts2csv(sorted_data, filename) # write CSV file

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

def verify_commit_order(org, repo): #----------------------------------------<<<
    """Verify that the GitHub API returns commits in chronological order.
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
    for commit in json_first:
        commit_date = commit['commit']['committer']['date']
        commit_msg = commit['commit']['message']
        print(commit_date, commit_msg)

    #if not lastpage_url:
    #    return pagesize # only one page of results, so we're done

    # get last page of results
    #lastpage = requests_session.get(lastpage_url,\
    #    headers={"Accept": "application/vnd.github.v3+json"})
    #json_last = json.loads(lastpage.text)
    #lastpage_count = len(json_last) # number of items on the last page

#----------------------------------------------------------------------------<<<
# TESTS                                                                      <<<
#----------------------------------------------------------------------------<<<

def test_commit_count_date(): #----------------------------------------------<<<
    """Test cases for commit_count_date()
    """
    testdata = [('microsoft/ghcrawler-datalake-etl', '2017-03-10'),
                ('microsoft/typescript', '2016-01-05'),
                ('microsoft/typescript', '2017-01-03'),
                ('microsoft/typescript', '2017-04-01'),
                ('dotnet/corefx', '2016-01-05'),
                ('dotnet/corefx', '2017-01-03'),
                ('dotnet/corefx', '2017-04-01')]
    for testrepo, testdate in testdata:
        org, repo = testrepo.split('/')
        github = commit_count_date(org, repo, testdate, 'g')
        datalake = commit_count_date(org, repo, testdate, 'c')
        print('{0:10}/{1:22} - {2} - Data Lake:{3:5} - GitHub:{4:5}'.format( \
            org, repo, testdate, datalake, github))

def test_commit_count_sincedate(): #-----------------------------------------<<<
    """Test cases for commit_count_sincedate()
    """
    testrepos = ['microsoft/typescript', 'microsoft/dotnet',
                 'microsoft/ghcrawler-datalake-etl']
    for orgrepo in testrepos:
        print(orgrepo)
        org, repo = orgrepo.split('/')
        print('- total commits: {0}'. \
            format(commit_count_sincedate(org, repo)))
        print('- since 1/1/2017: {0}'. \
            format(commit_count_sincedate(org, repo, '2017-01-01')))
        print('- since 3/20/2017: {0}'. \
            format(commit_count_sincedate(org, repo, '2017-03-20')))

def test_commits_asofdate(): #-----------------------------------------------<<<
    """Test cases for commits_asofdate()
    """
    #testcases = [('microsoft/typescript', '2017-04-03'),
    #             ('microsoft/vscode', '2017-04-03'),
    #             ('microsoft/ghcrawler-datalake-etl', '2017-04-03'),
    #             ('Azure/azure-github-organization', '2017-04-03')]
    #for orgrepo, asof in testcases:
    open('repo_commits_audit_2017-04-03.csv', 'w').write( \
        'org,repo,datalake,github\n')

    asof = '2017-04-03'
    for orgrepo in open('repos_in_repocsv.csv', 'r').readlines():
        orgrepo = orgrepo.lower().strip()
        org, repo = orgrepo.split('/')
        if not org == 'microsoft':
            continue
        if documentation_repo(repo):
            continue
        github_commits = commits_asofdate(org, repo, asof)
        datalake_commits = repototal_commits(org, repo)
        if datalake_commits == github_commits:
            desc = '-------'
        elif datalake_commits > github_commits:
            desc = 'extra'
        else:
            desc = 'MISSING'
        print(orgrepo.ljust(50) + ' - ' + asof + \
            ' - DataLake:{0:>6}, GitHub:{1:>6}'. \
            format(datalake_commits, github_commits) + ' ' + desc)
        open('repo_commits_audit_2017-04-03.csv', 'a').write( \
            ','.join([org, repo, str(datalake_commits), str(github_commits)]) + '\n')

# code to be executed when running standalone (for ad-hoc testing, etc.)
if __name__ == '__main__':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    daily_diff()
    #test_commit_count_date()
    #test_commit_count_sincedate()
    #linkingdata_update()
    #verify_commit_order('dmahugh', 'gitdata')

    #asof = '2017-04-03'
    #infile = 'verification_activities_repo.csv'
    #outfile = 'repototals-' + asof + '.csv'
    #repototals_asofdate(infile, outfile, asof)

    #test_commits_asofdate()

    """
    myreader = csv.reader(open('repo_commits_audit_2017-04-03.csv', 'r'),
                          delimiter=',', quotechar='"')
    next(myreader, None) # skip header
    for values in myreader:
        org = values[0]
        repo = values[1]
        datalake = int(values[2])
        github = int(values[3])

        percent = 100 if github == 0 else 100 * datalake/github

        if datalake == github:
            match = 'match'
        elif datalake > github:
            if percent < 101:
                match = '<1% extra'
            elif percent < 110:
                match = '<10% extra'
            else:
                match = '>10% extra'
        else:
            if percent > 99:
                match = '<1% missing'
            elif percent > 90:
                match = '<10% missing'
            else:
                match = '>10% missing'

        print(','.join([*values, str(round(percent, 0)), match]))
    """
