#! /usr/bin/env python


from pycommon.util.util import *
from pycommon.db.db import db

import pickle, string
from datetime import datetime

rdb = db("enwiki_p_local")
ldb = db("reflex_relations_2014")

output = "projectTotalEdits.txt"
f = open(output, "w")

def main():
    # First get all the projects
    query = "SELECT p_id, p_title FROM project"
    lc = ldb.execute(query)
    pQuery = []
    tQuery = []
    rows = lc.fetchall()
    for r in rows:
        pQuery.append(str(r["p_id"]))
        tQuery.append("page_title = '" + ldb.escape_string(lc, r['p_title']) + "' OR page_title LIKE '" + ldb.escape_string(lc, r['p_title']+"/%%") + "'")

    pEdits = 0
    nEdits = 0
    ptEdits = 0
    ntEdits = 0
    # Second, get all the edits to the project pages
    out("Fetching edits to project pages")
    query = "SELECT COUNT(rev_id) AS 'count' FROM revision WHERE rev_page IN (%s) AND rev_timestamp > '20130501000000' AND rev_timestamp' <= '20140501000000'" % (','.join(pQuery))
    rc = rdb.execute(query)
    row = rc.fetchone()
    pEdits += rc["count"]

    # And get all the edits to the non-project pages
    out("Fetching edits to non-project pages")
    query = "SELECT COUNT(rev_id) AS 'count' FROM revision WHERE rev_page NOT IN (%s) AND rev_timestamp > '20130501000000' AND rev_timestamp' <= '20140501000000'" % (','.join(pQuery))
    rc = rdb.execute(query)
    row = rc.fetchone()
    nEdits += rc["count"]

    # Next, grab the templates transcluded on project pages
    out("Fetching templates transcluded on project pages")
    query = "SELECT templatelinks.*, p.page_title, t.page_id FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from IN (" + ','.join(pQuery) + ")"
    rc = rdb.execute(query)
    rows = rc.fetchall()
    temps = []
    for r in rows:
        temps.append(str(r["page_id"]))
    # And get edits to these
    out("Fetching edits to templates transcluded on project pages")
    query = "SELECT COUNT(rev_id) AS 'count' FROM revision WHERE rev_page IN (%s) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000'" % (','.join(temps))
    rc = rdb.execute(query)
    row = rc.fetchone()
    ptEdits += rc["count"]

    # Next, grab the templates transcluded on non-project pages (in ns 4,5)
    out("Fetching templates transcluded on non-project pages")
    query = "SELECT templatelinks.*, p.page_title, t.page_id FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from NOT IN (" + ','.join(pQuery) + ") AND tl_from_namespace IN (4,5)"
    rc = rdb.execute(query)
    rows = rc.fetchall()
    temps = []
    for r in rows:
        temps.append(str(r["page_id"]))
    # nd get edits to these
    out("Fetching edits to templates transcluded on non-project pages")
    query = "SELECT COUNT(rev_id) AS 'count' FROM revision WHERE rev_page IN (%s) AND rev_timestamp > '20130501000000' AND rev_timestamp <= '20140501000000'" % (','.join(temps))
    rc = rdb.execute(query)
    row = rc.fetchone()
    ntEdits += rc["count"]

    # Output our totals
    p_str = "Total project edits (project, template, total): %s, %s, %s\n" % (pEdits, ptEdits, pEdits + ptEdits)
    n_str = "Total non-project edits (project, template, total) (in ns 4,5): %s, %s, %s\n" % (nEdits, ntEdits, nEdits + ntEdits)
    out(p_str)
    out(n_str)
    f.write(p_str)
    f.write(n_str)

    f.close()
    

if __name__ == "__main__":
    main()


