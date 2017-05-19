"""repodiff.py
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the MIT License.
"""
# Compares current repos in Data Lake ghinsightsms account against what's returned
# by the GitHub API, and generates a report of the differences found.

import collections
import configparser
import csv
import datetime
import glob
import json
import os
import pprint
import re
import sys
from timeit import default_timer

import requests

from shared import *

def data_sort(datadict): #---------------------------------------------------<<<
    """Sort function for output lists.

    takes an OrderedDict object as input, returns lower-case version of the
    first value in the OrderedDict, for use as a sort key.
    """
    sortkey = list(datadict.keys())[0]
    sortvalue = str(datadict[sortkey]).lower()
    return sortvalue

def github_data(*, endpoint=None, fields=None): #----------------------------<<<
    """Get data for specified GitHub API endpoint.

    endpoint     = HTTP endpoint for GitHub API call
    fields       = list of fields to be returned

    Returns a list of dictionaries containing the specified fields.
    Returns a complete data set - if this endpoint does pagination, all pages
    are retrieved and aggregated.
    """
    auth = (setting('ghinsights', 'github', 'username'), setting('ghinsights', 'github', 'pat'))
    all_fields = github_allpages(endpoint=endpoint, auth=auth)

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

def github_get_repos(filename): #--------------------------------------------<<<
    """Retrieve repo data from GitHub API and store as CSV file.
    """

    # get the list of orgs from local text file ...
    orglist = [_.lower().strip() for _ in open('data/orgs.txt').readlines()]

    repolist = [] # the list of repos
    for orgid in orglist:
        endpoint = '/orgs/' + orgid + '/repos?per_page=100'
        repolist.extend(github_data(endpoint=endpoint, \
            fields=['owner.login', 'name', 'id', 'created_at']))

    sorted_data = sorted(repolist, key=data_sort)
    dicts2csv(sorted_data, filename) # write CSV file

def print_log(logfile, text): #----------------------------------------------<<<
    """Print a a line of text and add it to the log file.
    """
    print(text)
    with open(logfile, 'a') as fhandle:
        fhandle.write(str(datetime.datetime.now())[:22] + ' ' + text + '\n')

def repo_data(filename): #---------------------------------------------------<<<
    """Load a repo CSV file into a list of tuples.

    Each tuple = (orgname, repo name, repo id, created)

    This function handles variations in file structure and returns a clean
    and consistent data set for comparison. All values are lower-case, and
    timestamp is trimmed to YYYY-MM-DD.
    """
    asofdate = get_asofdate()
    dataset = []
    encoding_type = 'ISO-8859-2' if 'datalake' in filename else 'UTF-8'
    firstline = 'github' in filename
    for line in open(filename, 'r', encoding=encoding_type).readlines():
        if firstline:
            # GitHub data file has a header row to be skipped
            firstline = False
            continue
        if line.strip().lower() == 'owner_login,name,created_at':
            continue # skip header in GitHub data file

        values = line.strip().split(',')

        if 'datalake' in filename:
            org = values[4].strip('"').upper()
            repo_name = values[3].strip('"').lower()
            repo_id = values[0].strip('"').split(':')[2]
            created_at = values[5][:10]
        else:
            org = values[0].upper()
            repo_name = values[1].lower()
            repo_id = values[2]
            created_at = values[3][:10]

        if documentation_repo(repo_name):
            continue # don't include documentation repos

        if created_at[:10] > asofdate:
            continue # don't include repos created after asofdate

        dataset.append((org, repo_name, repo_id, created_at))

    return dataset

def repo_diff(github, datalake, rptfile): #----------------------------------<<<
    """Diff two repo.csv files

    github = CSV file of repo data from the GitHub API (master copy)
    datalake = CSV file from ghinsightsms Azure Data Lake Store

    If filenames are not provided, defaults to today's files.

    Differences are displayed and also written to repodiff-YYYY-MM-DD.csv report
    file in the /data-verification folder.

    Returns a tuple of total repos (missing, extra).
    """

    repos_github = repo_data(github)
    repos_datalake = repo_data(datalake)

    missing = [] # repos missing from Data Lake
    extra = [] # repos in Data Lake but not GitHub

    # check for missing from Data Lake, or different created date
    for org, repo_name, repo_id, created in repos_github:
        created_dl = repo_found(repos_datalake, org, repo_id)
        if not created_dl:
            missing.append((org, repo_name, repo_id, created))

    # check for extra in Data Lake
    for org, repo_name, repo_id, created in repos_datalake:
        created_gh = repo_found(repos_github, org, repo_id)
        if not created_gh:
            extra.append((org, repo_name, repo_id, created))

    # write output file
    with open(rptfile, 'w') as outfile:
        outfile.write('org,repo_name, repo_id,created_at, issue\n')
        for org, repo_name, repo_id, created_at in sorted(missing):
            outfile.write(','.join([org, repo_name, repo_id, created_at, 'missing']) + '\n')
        for org, repo_name, repo_id, created_at in sorted(extra):
            outfile.write(','.join([org, repo_name, repo_id, created_at, 'extra']) + '\n')

    return (len(missing), len(extra))

def repo_found(dataset, org, repo): #----------------------------------------<<<
    """Check whether a dataset contains an org/repo. The passed repo value
    is the id, not the name.
    """
    for orgname, repo_name, repo_id, createddate in dataset:
        if orgname == org and repo_id == repo:
            return createddate
    return False

def yyyy_mm_dd(): #----------------------------------------------------------<<<
    """Return current date as a string, in YYYY-MM-DD format."""
    # note that we assume US locale
    return str(datetime.datetime.now())[:10]

# code to be executed when running standalone ...
if __name__ == '__main__':

    # set console encoding to UTF-8 (for printing )
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

    start_time = default_timer()
    datestr = get_asofdate()
    datalake_csv = '/TabularSource2/Repo.csv'
    datalake_csv_local = 'data/repo-datalake-' + datestr + '.csv'
    github_csv = 'data/repo-github-' + datestr + '.csv'
    diff_file = 'data-verification/repodiff-' + datestr + '.csv'
    diff_file_datalake = '/users/dmahugh/repodiff.csv'
    logfile = 'data-verification/repodiff.log'
    logfile_datalake = '/users/dmahugh/repodiff.log'

    print_log(logfile,
              10*'-' + ' Data Verification for ' + datestr + ' ' + 10*'-')

    # download the current CSV file from Azure Data Lake Store
    download_start = default_timer()
    token, _ = azure_datalake_token('ghinsights')
    adls_account = setting('ghinsights', 'azure', 'adls-account')
    datalake_get_file(datalake_csv_local, datalake_csv, adls_account, token)
    print_log(logfile, 'Download Repo.csv from Data Lake '.ljust(40, '.') + \
        '{0:6.1f} seconds, {1:,} bytes'.format( \
        default_timer() - download_start, filesize(datalake_csv_local)))

    # get current results from GitHub API
    github_start = default_timer()
    github_get_repos(github_csv)
    print_log(logfile, 'Get live data from GitHub API '.ljust(40, '.') +
              '{0:6.1f} seconds, {1:,} bytes'.format( \
        default_timer() - github_start, \
        filesize(github_csv)))

    # generate diff report
    diff_start = default_timer()
    missing, extra = repo_diff(github=github_csv,
                               datalake=datalake_csv_local,
                               rptfile=diff_file)
    print_log(logfile, ('Generate ' + os.path.basename(diff_file) + ' ').ljust(40, '.') +
              '{0:6.1f} seconds, {1:,} bytes'.format( \
        default_timer() - diff_start, filesize(diff_file)))
    if missing:
        print_log(logfile, 24*' ' + 'Missing: {:7,} Repos'.format(missing))
    if extra:
        print_log(logfile, 24*' ' + 'Extra:   {:7,} Repos'.format(extra))

    # upload diff report CSV file to Azure Data Lake Store
    upload_start = default_timer()
    token, _ = azure_datalake_token('ghinsights')
    adls_account = setting('ghinsights', 'azure', 'adls-account')
    datalake_put_file(diff_file, diff_file_datalake, adls_account, token)
    print_log(logfile, 'Upload repodiff.csv to Data Lake '.ljust(40, '.') +
              '{0:6.1f} seconds'.format(default_timer() - upload_start))

    print_log(logfile, 'Total elapsed time '.ljust(40, '-') + '{:6.1f} seconds'. \
        format(default_timer() - start_time))

    # upload log file to Azure Data Lake Store
    upload_start = default_timer()
    datalake_put_file(logfile, logfile_datalake, adls_account, token)
    print('Update repodiff.log on Data Lake '.ljust(40, '.') +
          '{0:6.1f} seconds'.format(default_timer() - upload_start))
