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
This script is intended to get all REVISION COMMENTS to edits to project pages, sub-pages, 
or templates transcluded on either of the above, as well as all corresponding talk pages.
The goal is to count the number of times a starting or ending phrase occurs in each of these,
indicating that the edits were potentially tool-mediated (so NOT including bot edits).
"""

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

import pickle, string
from collections import Counter

db = db()
csv = "projectNonBotComments.csv"
f = open(csv, "w")
f.write("name,timestamp,edited_page,edited_namespace,project_page,is_project_edit,rev_comment\n")
out("Creating csv: " + csv)

def pickle_struc(f="getProjComments.dat", d={}):
    fout = open(f, "w")
    pickle.dump(d, fout)
    return True

def unpickle_struc(f="getProjComments.dat"):
    fin = open(f, "r")
    d = pickle.load(fin)
    return d


def main():
    lc = db.getCursorForDB("reflex_relations_2014")
    rc = db.getCursorForDB("enwiki_p_local")

    # First, get all the projects
    query = "SELECT p_id, p_title FROM project"
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    projects = {}
    pQuery = []
    out("Fetching WikiProjects...")
    for r in rows:
        projects[r['p_id']] = r['p_title']
        pQuery.append("page_title = '" + db.escape(lc, r['p_title']) + "' OR page_title LIKE '" + db.escape(lc, r['p_title']+"/%%") + "'")

    # Second, get edits to project pages or sub-pagess
    out("Fetching edits to WikiProjects...")
    query = "SELECT /* SLOW_OK */ rev_id, rev_user_text, rev_timestamp, page_title, page_namespace, rev_comment, ug_group FROM revision LEFT JOIN user_groups ON rev_user = ug_user JOIN page ON rev_page = page_id WHERE (ug_group != 'bot' OR ug_group IS NULL) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000' AND page_namespace IN (4,5) AND (%s) GROUP BY rev_id" % (' OR '.join(pQuery))
    rc = db.execute(rc, query)
    rows = rc.fetchall()
    out("Writing project edits...")
    pickle_struc(f="projectComment_revisions.dat", d=rows)
    #rows = unpickle_struc(f="projectComment_revisions.dat")
    comments = []
    for r in rows:
        # name,timestamp,edited_page,edited_namespace,project_page,is_project_edit,rev_comment
        r["rev_comment"] = r["rev_comment"].replace('"', "'")
        r["rev_user_text"] = r["rev_user_text"].replace('"', "'")
        r["page_title"] = r["page_title"].replace('"', "'")
        comments.append(r["rev_comment"])
        f.write('"' + r["rev_user_text"] + '",' + str(r["rev_timestamp"]) + ',"' + r["page_title"] + '",' + str(r["page_namespace"]) + ',"' + r["page_title"] + '",1,"' + r["rev_comment"] + '"\n')

    # Third, get bots that have edited templates which are transcluded on project pages or sub-pages
    # Note: templatelinks table tl_from should be the id of the project or sub-page, tl_namespace and
    # tl_title will be what we want to see if bots edited, as /that's/ what's going to show up in the
    # tl_from page (the WP page).
    # So, first get all the templates that are transcluded on project pages
    out("Fetching templates transcluded on project pages...")
    query = "SELECT templatelinks.*, p.page_title, t.page_id FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from in (" + ','.join(str(x) for x in projects.keys()) + ")"
    rc = db.execute(rc, query)
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
    out("Fetching edits to project templates...")
    query = "SELECT /* SLOW_OK */ rev_id, rev_user_text, rev_timestamp, rev_page, page_title, page_namespace, rev_comment, ug_group FROM revision LEFT JOIN user_groups ON rev_user = ug_user JOIN page ON rev_page = page_id WHERE (ug_group != 'bot' OR ug_group IS NULL) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000' AND rev_page IN (%s) GROUP BY rev_id" % (','.join(str(x) for x in temps.keys()))
    rc = db.execute(rc, query)
    rows = rc.fetchall()
    pickle_struc(f="projectComments_Transcluded.dat", d=rows)
    #rows = unpickle_struc(f="projectComments_Transcluded.dat")
    out("Writing template edits...")
    for r in rows:
        r["rev_comment"] = r["rev_comment"].replace('"', "'")
        r["rev_user_text"] = r["rev_user_text"].replace('"', "'")
        r["page_title"] = r["page_title"].replace('"', "'")
        comments.append(r["rev_comment"])
        f.write('"' + r["rev_user_text"] + '",' + str(r["rev_timestamp"]) + ',"' + r["page_title"] + '",' + str(r["page_namespace"]) + ',"' + '|'.join(temps[r["rev_page"]]) + '",0,"' + r["rev_comment"] + '"\n')

    f.close()
    out("Completed writing csv: " + csv)

    pickle_struc(f="projectNonBotComments.dat", d=comments)

if __name__ == "__main__":
    main()

