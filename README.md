# MathspringDataProcessing
A collection of scripts for processing [Mathspring](https://github.com/marshall62/mathspring) data. This is intended as a starting point for anyone who wants to work with Mathspring's data.

To get it running, all you should need to do is:
- Have Python installed
- Clone the repository
- Get the necessary modules (such as with "pip install -r requirements.txt")
- Run one of the data processing scripts ("python dec2016-empathy.py")

If you have a database login, you can provide the credentials in a text file "database_user.txt", which should contain the username on the first line and the password on the second line. This will allow you to change the queries to get your own new sets of data. Otherwise, you can run a script with a saved data folder to do the processing with cached query results.

Note that the database structure changes so the most recent script is most likely to reflect that and run properly.

Script List (from most recent to least recent)- <br>
1. dec2016-empathy.py - Extracts event log for the specified date intervals and class IDs. Refer to the comments in the script for inputs.
