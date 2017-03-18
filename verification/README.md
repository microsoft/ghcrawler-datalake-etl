# GHCrawler data verification
This folder contains tools for verifying the completeness and accuracy of the data
being accumulated by GHCrawler.

## general approach

The general concept is that CSV files are generated from key steps in the data
pipeline, and the contents of these files are compared against the current results
returned by the GitHub API.

This is a work in progress, and more thorough documentation will be provided later.

## authentication details

Note that ```ghiverify.py``` requires authentication for Azure Data Lake access
(via Azure Active Directory) and the GitHub API (via a username/personal access token).
These details are stored in a local file named ```ghiverify.ini``` that is stored in a
sibling folder (subdir of the parent) named ```_private```. Here is an example of the
structure of the INI file:

```
[github]
username = xxxxxxxx
pat = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

[azure]
subscription = xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
adls-account = xxxxxxxxxxx

[aad]
tenant-id = xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
client-secret = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=

client-id = xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```