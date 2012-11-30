#!/usr/bin/python
# db.py - create animal table and
# retrieve information from it

import sys
import MySQLdb

# connect to the MySQL server

global conn
global TABLE_RSS
global TABLE_SCRAPY

try:
    conn = MySQLdb.connect (host = "localhost",
                         user = "root",
                         passwd = "54321",
                         db = "django_crawler")
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit (1)



