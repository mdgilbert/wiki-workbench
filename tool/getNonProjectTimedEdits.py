#! /usr/bin/env python

# Copyright 2013 Mdgilbert

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This script is intended to identify revisions by individual users which occur subsequently in
FIVE SECONDS OR LESS, intended to help identify bot or tool mediated edits.
"""

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

import pickle, string
from datetime import datetime

rdb = db("enwiki_p_local")
ldb = db("reflex_relations_2014")
csv = "nonProjectNonBotTimed.csv"
f = open(csv, "w")

# print the header
f.write("name,timestamp,edited_page,edited_namespace,project_page,is_project_edit,rev_comment\n")
out("Creating csv: " + csv)

def pickle_struc(f="getProjTimed.dat", d={}):
    fout = open(f, "w")
    pickle.dump(d, fout)
    return True

def unpickle_struc(f="getProjTimed.dat"):
    fin = open(f, "r")
    d = pickle.load(fin)
    return d


def main():
    # First, get all the projects
    query = "SELECT p_id, p_title FROM project"
    lc = ldb.execute(query)
    rows = lc.fetchall()
    projects = {}
    nQuery = []
    tQuery = []
    out("Fetching WikiProjects...")
    for r in rows:
        projects[r['p_id']] = r['p_title']
        nQuery.append(str(r['p_id']))
        tQuery.append("page_title != '" + ldb.escape_string(r['p_title']) + "' AND page_title NOT LIKE '" + ldb.escape_string(r['p_title']+"/%%") + "'")

    # Second, get edits to NON project pages or sub-pagess
    out("Fetching edits to non-WikiProjects...")
    query = "SELECT /* SLOW_OK */ rev_id, rev_user_text, rev_timestamp, page_title, page_namespace, rev_comment, ug_group FROM revision LEFT JOIN user_groups ON rev_user = ug_user JOIN page ON rev_page = page_id WHERE (ug_group != 'bot' OR ug_group IS NULL) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000' AND page_namespace IN (4,5) AND (%s) GROUP BY rev_id ORDER BY rev_user_text, rev_id ASC" % (' OR '.join(tQuery))
    rc = rdb.execute(query)
    rows = rc.fetchall()

    out("Writing non-project edits...")
    pickle_struc(f="nonProjectEdits_TimedToProject.dat", d=rows)
    #rows = unpickle_struc(f="projectEdits_TimedToProject.dat")
    lastUser = ""
    lastTime = 20010101000000
    l = []
    tool = False
    for r in rows:
        # If the user is the same as the prior user and the time between edits is <= 5 seconds, write line
        if not r["rev_comment"]:
            r["rev_comment"] = ""
        else:
            r["rev_comment"] = r["rev_comment"].replace('"', "'")
        if not r["rev_user_text"]:
            r["rev_user_text"] = ""
        else:
            r["rev_user_text"] = r["rev_user_text"].replace('"', "'")
        r["page_title"] = r["page_title"].replace('"', "'")

        # name,timestamp,edited_page,namespace,project_page,isProject,comment
        delta = datetime.strptime(str(r["rev_timestamp"]), "%Y%m%d%H%M%S") - datetime.strptime(str(lastTime), "%Y%m%d%H%M%S")
        if r["rev_user_text"] == lastUser and delta.seconds <= 5:
            tool = True
            f.write('"' + l["rev_user_text"] + '",' + str(l["rev_timestamp"]) + ',"' + l["page_title"] + '",' + str(l["page_namespace"]) + ',"' + l["page_title"] + '",1,"' + l["rev_comment"] + '"\n')
        elif tool == True:
            tool = False
            f.write('"' + l["rev_user_text"] + '",' + str(l["rev_timestamp"]) + ',"' + l["page_title"] + '",' + str(l["page_namespace"]) + ',"' + l["page_title"] + '",1,"' + l["rev_comment"] + '"\n')

        lastUser = r["rev_user_text"]
        lastTime = r["rev_timestamp"]
        l = r

    # Third, get bots that have edited templates which are transcluded on non-project pages or sub-pages
    # Note: templatelinks table tl_from should be the id of the project or sub-page, tl_namespace and
    # tl_title will be what we want to see if bots edited, as /that's/ what's going to show up in the
    # tl_from page (the WP page).
    # So, first get all the templates that are transcluded on project pages
    out("Fetching templates transcluded on non-project pages...")
    query = "SELECT templatelinks.*, p.page_title, t.page_id FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from NOT IN (" + ','.join(nQuery) + ") AND tl_from_namespace IN (4,5)"
    rc = rdb.execute(query)
    rows = rc.fetchall()
    #pickle_struc(f="projectTemps.dat", d=rows)
    #rows = unpickle_struc(f="projectTemps.dat")
    temps = {}
    for r in rows:
        if r["page_id"] in temps:
            temps[r["page_id"]].append(r["page_title"])
        else:
            temps[r["page_id"]] = [r["page_title"]]

    # Then, build the query to see if bots edited any of the page/namespaces previously fetched
    out("Fetching edits to non-project templates...")
    query = "SELECT /* SLOW_OK */ rev_id, rev_user_text, rev_timestamp, rev_page, page_title, page_namespace, rev_comment, ug_group FROM revision LEFT JOIN user_groups ON rev_user = ug_user JOIN page ON rev_page = page_id WHERE (ug_group != 'bot' OR ug_group IS NULL) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000' AND rev_page NOT IN (%s) GROUP BY rev_id ORDER BY rev_user_text, rev_id ASC" % (','.join(nQuery))
    rc = rdb.execute(query)
    rows = rc.fetchall()
    #pickle_struc(f="projectTimed_Templates.dat", d=rows) # Would be the same as projectComments
    #rows = unpickle_struc(f="projectTimed_Templates.dat")
    out("Writing template edits...")
    lastUser = ''
    lastTime = 20010101000000
    l = []
    tool = False
    for r in rows:
        # If the user is the same as the prior user and the time between edits is <= 5 seconds, write line
        r["rev_comment"] = r["rev_comment"].replace('"', "'")
        r["rev_user_text"] = r["rev_user_text"].replace('"', "'")
        r["page_title"] = r["page_title"].replace('"', "'")

        # name,timestamp,edited_page,namespace,project_page,isProject,comment
        delta = datetime.strptime(str(r["rev_timestamp"]), "%Y%m%d%H%M%S") - datetime.strptime(str(lastTime), "%Y%m%d%H%M%S")
        if r["rev_user_text"] == lastUser and delta.seconds <= 5:
            tool = True
            f.write('"' + l["rev_user_text"] + '",' + str(l["rev_timestamp"]) + ',"' + l["page_title"] + '",' + str(l["page_namespace"]) + ',"' + '|'.join(temps[l["rev_page"]]) + '",0,"' + l["rev_comment"] + '"\n')
        elif tool == True:
            tool = False
            f.write('"' + l["rev_user_text"] + '",' + str(l["rev_timestamp"]) + ',"' + l["page_title"] + '",' + str(l["page_namespace"]) + ',"' + '|'.join(temps[l["rev_page"]]) + '",0,"' + l["rev_comment"] + '"\n')


        lastUser = r["rev_user_text"]
        lastTime = r["rev_timestamp"]
        l = r


    f.close()

if __name__ == "__main__":
    main()

