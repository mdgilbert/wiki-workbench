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

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

db = db()
lc = db.getCursorForDB("reflex_relations_2014")
rc = db.getCursorForDB("enwiki_p_local")

def main():
    # First, get all the projects
    query = "SELECT p_id, p_title FROM project"
    lc = db.execute(lc, query)
    rows = lc.fetchall()
    projects = {}
    pQuery = []
    for r in rows:
        projects[r['p_id']] = r['p_title']
        pQuery.append("(page_title = '%s' OR page_title LIKE '%s/%')")

    # Second, get bots that have edited project pages or sub-pages
    # select rev_user_text, sum(rev_id) as 'edits', page_title from revision join user_groups on rev_user = ug_user join page on rev_page = page_id where ug_group = 'bot' and rev_timestamp > '20110401000000' and (page_title = 'WikiProject_Cats' or page_title like 'WikiProject_Cats/%') and page_namespace = 4 group by rev_user_text limit 10;
    query = "SELECT rev_user_text, SUM(rev_id) AS 'edits', page_title FROM revision JOIN user_groups ON rev_user = ug_user JOIN page ON rev_page = page_id WHERE ug_group = 'bot' AND rev_timestamp > '20130401000000' AND page_namespace = 4 AND (%s) GROUP BY rev_user_text" % (' OR '.join(pQuery))
    print(query)


if __name__ == "__main__":
    main()

