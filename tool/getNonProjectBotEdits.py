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
Goal: To collect all bot edits to project pages in order to identify first, WHICH bots edit
in the project space and, second, WHAT do those bots do (ie, do they serve in a coordinative 
function).  This should be at the resolution of individual edits so they can be charted over
time.

From the paper:
1) Edits by users with the "bot" flag: To be an officially recognized bot in Wikipedia, a unique bot account must be created and a request must be made with the Bot Approvals Group where the tasks that the bot aims to complete and the code that will enact those changes can be reviewed, vetted, and ultimately approved or rejected.  Once a request is approved, the user account will be added to the "bot" user group, effectively flagging that account as an official legitimate alternative account, capable of editing the encyclopedia in an automated or semi-automated fashion.  Edits by these accounts are easily identifiable and provide a simple starting point to aggregating the types of activities that automated and semi-automated users complete within the scope of WikiProjects.
"""

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

import pickle, sys, codecs

db = db()
csv = "projectBotEdits_nonProject_byBotNamespace.csv"
f = codecs.open(csv, "w", encoding="utf-8")
out("Creating csv: " + csv)

# Print csv file header
#f.write("name,timestamp,edited_page,edited_namespace,project_page,is_project_edit,rev_comment\n")
f.write("name,edits,namespace,is_wp_edit\n")

def pickle_struc(f="getProjEdits.dat", d={}):
    fout = open(f, "w")
    pickle.dump(d, fout)
    return True

def unpickle_struc(f="getProjEdits.dat"):
    fin = open(f, "r")
    d = pickle.load(fin)
    return d


def main():
    lc = db.getCursorForDB("reflex_relations_2014")
    rc = db.getCursorForDB("enwiki_p_local")

    # First, get all the projects
    query = "SELECT p_id, p_title FROM project"
    out("Fetching WikiProjects...")
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    projects = {}
    pQuery = []
    out("Found " + str(len(rows)) + " WikiProject pages (which will be excluded from bot edit counts)")
    for r in rows:
        projects[r['p_id']] = r['p_title']
        pQuery.append("tp_title != '" + db.escape(lc, r['p_title']) + "' OR tp_title NOT LIKE '" + db.escape(lc, r['p_title']+"/%%") + "'")

    # Second, get edits to project pages or sub-pages
    # Get from local db, cache currently up to ww 697, 2014-05-12
    # Wikiweeks from 20130501 to 20140501 are 643 - 695, inclusive
    out("Fetching edits to non-WikiProjects...")
    query = "SELECT tu_name, SUM(rc_edits) AS 'edits', tp_namespace FROM reflex_cache LEFT JOIN ts_users_groups ON rc_user_id = tug_uid JOIN ts_users ON tu_id = rc_user_id JOIN ts_pages ON rc_page_id = tp_id WHERE tug_group = 'bot' AND tp_namespace IN (4,5) AND rc_wikiweek >= 643 AND rc_wikiweek <= 695 AND (%s) GROUP BY tu_name, tp_namespace" % (' OR '.join(pQuery))
    lc = db.execute(lc, query)
    out("Writing bot edits to non-WikiProjects...")
    while True:
        rows = lc.fetchmany(10000)
        if not rows:
            break
        csvOut = ''
        for row in rows:
            row["tu_name"] = row["tu_name"].replace('"', "'").decode("utf-8")
            csvOut += '"' + row["tu_name"] + '",' + str(row["edits"]) + ',' + str(row["tp_namespace"]) + ',1\n'
        f.write(csvOut)
        sys.stdout.write('.')

    # Third, get bots that have edited templates which are transcluded on project pages or sub-pages
    # Note: templatelinks table tl_from should be the id of the project or sub-page, tl_namespace and
    # tl_title will be what we want to see if bots edited, as /that's/ what's going to show up in the
    # tl_from page (the WP page).
    # So, first get all the templates that are transcluded on project pages
    out("Fetching templates transcluded on project pages...")
    query = "SELECT templatelinks.*, p.page_title, p.page_namespace, t.page_id FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from in (" + ','.join(str(x) for x in projects.keys()) + ")"
    rc = db.execute(rc, query)
    rows = rc.fetchall()
    #pickle_struc(f="projectEdits_Templatess.dat", d=rows)
    #rows = unpickle_struc(f="projectEdits_Templates.dat")
    temps   = {}
    temp_ns = {}
    for r in rows:
        temp_ns[r["page_id"]] = r["page_namespace"]
        if r["page_id"] in temps:
            temps[r["page_id"]].append(r["page_title"])
        else:
            temps[r["page_id"]] = [r["page_title"]]

    # Then, build the query to see if bots edited any of the page/namespaces previously fetched
    out("Fetching edits to project templates...")
    query = "SELECT tu_name, SUM(rc_edits) AS 'edits', tp_namespace, tp_id FROM reflex_cache LEFT JOIN ts_users_groups ON rc_user_id = tug_uid JOIN ts_users ON tu_id = rc_user_id JOIN ts_pages ON rc_page_id = tp_id WHERE tug_group = 'bot' AND rc_wikiweek >= 643 AND rc_wikiweek <= 695 AND rc_page_id IN (%s) GROUP BY tu_name, tp_namespace" % (','.join(str(x) for x in temps.keys()))
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    #pickle_struc(f="projectEdits_Transcluded.dat", d=rows)
    #rows = unpickle_struc(f="projectEdits_Transcluded.dat")
    out("Writing template edits...")
    for row in rows:
        row["tu_name"] = row["tu_name"].replace('"', "'").decode("utf-8")
        f.write('"' + row["tu_name"] + '",' + str(row["edits"]) + ',' + str(temp_ns[row["tp_id"]]) + ',0\n')

    f.close()
    out("Finished writing csv: " + csv)

if __name__ == "__main__":
    main()

