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
Goal: To get counts of pages in the WP and WP Talk namespace,
discriminating between WikiProject and non-WP pages.
"""

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

import pickle, sys, codecs

db = db()

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
    npQuery= []
    out("Found " + str(len(rows)) + " WikiProject pages (next, checking all sub-pages under and not under these projects)")
    for r in rows:
        projects[r['p_id']] = r['p_title']
        npQuery.append("tp_title != '" + db.escape(lc, r['p_title']) + "' OR tp_title NOT LIKE '" + db.escape(lc, r['p_title']+"/%%") + "'")
        pQuery.append("tp_title = '" + db.escape(lc, r['p_title']) + "' OR tp_title LIKE '" + db.escape(lc, r['p_title']+"/%%") + "'")

    query = "SELECT COUNT(*) AS 'count' FROM ts_pages WHERE tp_namespace IN (4,5) AND (%s)" % (' OR '.join(pQuery))
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    for row in rows:
        out("Total pages under WikiProjects: " + str(row['count']))

    query = "SELECT COUNT(*) AS 'count' FROM ts_pages WHERE tp_namespace IN (4,5) AND (%s)" % (' OR '.join(npQuery))
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    for row in rows:
        out("Total pages NOT under WikiProjects: " + str(row['count']))



if __name__ == "__main__":
    main()

