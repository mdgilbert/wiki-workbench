#! /usr/bin/env python

# Copyright 2015 Mdgilbert

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
Goal: To collect all edits and interactions which may fall under the taxonomy
for a modified theory of coordination. Categories in this taxonomy and the
operationalization for how Wikipedia interactions map to each category follow
below -

Group identification:
* Project selection - Placing a member link on a project page, sub-page, or any
  page transcluded on either, excluding Talk pages.
* Outreach - Placing a link on a Project talk page that links to a different project.

Role identification: 
* Selecting coordination/production activities - Implicit in switch between tasks, i.e., 
  editing articles or posting to article talk pages in succession.

Group awareness:
* Awareness of content under group scope - Indicated by edits to an article that was
  linked to by a third party on a project page, sub-page, template transcluded on
  either, or corresponding Talk page.

Cultural awareness:
*

Social awareness:
* When an edit is made to a project talk page or a sub-talk page for a project, 
  all other editors who have edited that page /in other section threads/ indicate 
  implicit awareness for the originating editor. In other words, the original 
  editor implicitly would see those non-related threads which would increase 
  their social awareness of group activity.

Coordination activity:
* Activity required to sustain the group - Posting to project pages or sub-pages,
  excluding talk pages for either.

Social coordination:
* Posting to project talk pages or sub-talk pages underneath a top-level talk
  page section. In other words, the top-level talk page post will not count as
  social coordination, but each post underneath that top level /will/ count as
  social coordination.

Production activity:
* Edits to articles under the scope of a given project. Data collected should 
  differentiate by edits made by project members versus non-members, however that
  distinction may not be reflected in the final diagrams.

Social production:
* Edits made to talk pages of articles under the scope of a project. Similar
  to Social coordination, this will count edits under a thread starting post
  for each thread on a talk page, but will exclude the original thread starting
  post.


Projects were selected for data collection manually from the list of projects
encountered during the interview process. Care was taken to ensure that
projects included covered a broad spectrum of goals, from the topic-centric 
projects with the primary goal of improving article content (such as WikiProject
Medicine) as well as task-centric projects with the primary goal of improving
general processes above the scope of any single project (such as WikiProject
Copyright cleanup). In total, 10 projects were selected for analysis covering
a broad range of topic and task oriented subjects to illustrate the wide variety
of work which occurs across these distributed teams. These are defined below.

"""

wiki_projects = [
    # Topic-oriented projects
    "WikiProject_Feminism",
    "WikiProject_Piracy",
    "WikiProject_Medicine",
    "WikiProject_Plants",
    "WikiProject_Chemistry",
    # Task-oriented projects
    "WikiProject_Spoken_Wikipedia",
    "WikiProject_Countering_systemic_bias",
    "WikiProject_Copyright_Cleanup",
    "WikiProject_Missing_encyclopedic_articles",
    "WikiProject_Outreach"
]

# Import local tools
from pycommon.util.util import *
from pycommon.db.db import db

import pickle, sys, codecs
import operator

if len(sys.argv) < 2:
    print("Enter project name:")
    for project in wiki_projects:
        print("  %s" % (project))

localDb = "reflex_relations_2014"
remoteDb = "enwiki_p_local"
ldb = db(localDb, "ldb")
rdb = db(remoteDb, "rdb")

all_project_page_ids = []
all_project_talk_page_ids = []

def pickle_struc(f="getCoordEdits.dat", d={}):
    fout = open(f, "w")
    pickle.dump(d, fout)
    return True

def unpickle_struc(f="getCoordEdits.dat"):
    fin = open(f, "r")
    d = pickle.load(fin)
    return d

def getRedirectTo(page):
    query = "SELECT page_id, page_namespace, page_title, page_is_redirect FROM redirect JOIN page ON rd_title = page_title AND rd_namespace = page_namespace WHERE rd_from = %s"
    rc = rdb.execute(query, (page["page_id"]))
    row = rc.fetchone()
    # If we couldn't find the redirect page we'll need to skip this page
    if not row:
        page["page_is_redirect"] == -1
        row = page
    elif row["page_is_redirect"] == 1:
        return getRedirectTo(row)
    return row

def getCoordinationEdits(row):
    """ 
    The goal of this is to identify and record all potential dependencies
    identified within project-related interactions. Since these will need
    to be recorded chronologically across all dependencies we'll need to 
    retrieve all relevant interactions in order (i.e., since we're creating
    the dependency trajectory diagram we'll want to know each time one
    dependency leads in to the next, or to itself as the case may be).
    """

    dependencies = {}
    dep_by_user = {}

    # First, grab all the related pages and templates for this project.
    out("[%s] Fetching project-related pages, sub-pages, and talk pages" % (row["p_title"]))
    query = "SELECT page_id, page_title, page_namespace, page_is_redirect FROM page WHERE page_namespace IN (4,5) AND (page_title = %s OR page_title LIKE %s) ORDER BY page_title ASC"
    rc = rdb.execute(query, (row["p_title"], row["p_title"] + "/%%"))
    pages = rc.fetchall()
    project_page_ids = []
    project_talk_page_ids = []
    for page in pages:
        # Make sure we grab redirect pages
        if page["page_is_redirect"] == 1:
            page = getRedirectTo(page)
        # If we couldn't find a target page for a redirect, skip this page
        if page["page_is_redirect"] == -1:
            continue
        # Save the page id
        if page["page_namespace"] % 2 == 0:
            project_page_ids.append(page["page_id"])
        else:
            project_talk_page_ids.append(page["page_id"])

        # Then, grab all templates transcluded on this project-related page
        query = "SELECT t.page_id, t.page_title, t.page_namespace, t.page_is_redirect FROM templatelinks JOIN page AS p ON tl_from = p.page_id JOIN page AS t ON tl_title = t.page_title AND tl_namespace = t.page_namespace WHERE tl_from = %s GROUP BY page_id"
        rc = rdb.execute(query, (page["page_id"]))
        templates = rc.fetchall()
        for template in templates:
            # Also grab potential redirects
            if template["page_is_redirect"] == 1:
                template = getRedirectTo(template)
            # If we couldn't find a target redirect, skip this template
            if template["page_is_redirect"] == -1:
                continue
            # Save the template id
            if template["page_namespace"] % 2 == 0:
                project_page_ids.append(template["page_id"])
            else:
                project_talk_page_ids.append(template["page_id"])

    # Next, grab all pages under the scope of the project
    out("[%s] Fetching pages under the scope of the project" % (row["p_title"]))
    query = "SELECT pp_id, tp_namespace FROM project_pages JOIN ts_pages ON pp_id = tp_id WHERE pp_project_id = %s"
    lc = ldb.execute(query, (row["p_id"]))
    pages = lc.fetchall()
    scope_page_ids = []
    scope_talk_page_ids = []
    for page in pages:
        # Save the page id
        if page["tp_namespace"] % 2 == 0:
            scope_page_ids.append(page["pp_id"])
        else:
            scope_talk_page_ids.append(page["pp_id"])

    print("\n\n")

    ####
    ## Group identification - Project selection, adding a link on a project page back to a 
    ##   user page.
    ####

    # Now, *page_ids contains all the pages, sub-pages, templates transcluded on those pages, and 
    # all corresponding talk pages for this project. The next step will be to 
    # Get all the member links for this project. We'll want to know 
    # each time a user link was added to a project page or sub-page
    # as well as the specific date it was added. 
    out("[%s] Group identification - Membership" % (row["p_title"]))
    out("[%s] Fetching user links on project pages" % (row["p_title"]))
    query = "SELECT * FROM coord_project_links WHERE cpl_page_id IN (%s) AND cpl_link_date >= '%s' AND cpl_link_page_namespace = 2 GROUP BY cpl_link_page_text, cpl_link_date ORDER BY cpl_link_date ASC" % (",".join(map(str, project_page_ids)), row["p_created"])
    #lc = ldb.execute(query, (",".join(map(str, project_page_ids)), row["p_created"]))
    lc = ldb.execute(query)
    links = lc.fetchall()

    # The coord_project_links table will return a list of all links present for each revision
    # of the project related page. We'll want to structure the format of the data to only
    # include when a link was /added/.
    last_rev_members = {}
    this_rev_members = {}
    members_at = {}
    last_date = 0
    last_ww = 0

    # Somewhat hacky, first run through to determine members_at
    last_members = {}
    this_members = {}
    last_rev = 0
    for link in links:
        # Skip sub-pages
        if link["cpl_link_page_text"].find("/") is not -1:
            continue

        this_rev = link["cpl_link_rev"]
        this_date = link["cpl_link_date"]
        this_member = link["cpl_link_page_text"]
        this_page_id = link["cpl_link_page_id"]
        this_ww = date_to_ww(this_date)
        this_member_id = getMemberId(this_member)

        # If we're on a new revision, we'll want to parse differences between
        # last_members and this_members to see if any links were /added/. I.e.,
        # if there is a link in this_members that's not in last_members,
        # add a Group identification dependency for this timestamp.
        if last_rev is not this_rev:
            for member in this_members:
                if member not in last_members:
                    # We've found a new Group identification dependency
                    if this_date not in dependencies:
                        dependencies[this_date] = []
                    dependencies[this_date].append("Group identification")

                    if this_member_id not in dep_by_user:
                        dep_by_user[this_member_id] = {}
                    if this_date not in dep_by_user[this_member_id]:
                        dep_by_user[this_member_id][this_date] = []
                    dep_by_user[this_member_id][this_date].append("Group identification")

            # Once we've gone through the members and identified dependencies,
            # copy this_members to last_members, clear out this_members, and
            # update last_rev
            last_members = this_members.copy()
            this_members = {}
            last_rev = this_rev

        # /After/ we've added dependencies if the revision shifted, add
        # members to this_members
        this_members[this_member] = this_member_id

        # Finally, start building the members_at dict. This will be completed
        # in a separate loop once all links are parsed.
        if this_ww not in members_at:
            members_at[this_ww] = {}
        members_at[this_ww][this_member] = this_member_id

    # Once we've gone through all the links, we'll need to fill in all the 
    # weeks in which no revisions were made to complete the members_at dict.
    # I.e., if there is a revision in week 20 and 25, all members in 21 - 24
    # should be the same as 20.
    min_ww = date_to_ww(links[0]["cpl_link_date"])
    end_ww = date_to_ww(links[-1]["cpl_link_date"])
    this_ww = min_ww
    last_members = {}

    out("[%s] Start ww: %s, End ww: %s" % (row["p_title"], this_ww, end_ww))

    while this_ww <= end_ww:
        if this_ww not in members_at:
            members_at[this_ww] = last_members
        else:
            last_members = members_at[this_ww].copy()
        this_ww += 1

    out("[%s] Length members_at: %s" % (row["p_title"], len(members_at)))

    print("\n\n")

    ####
    ## Group identification - Outreach, placing a link on a project talk page that 
    ##   links to a different project.
    ####
    """
    out("[%] Group identification - Outreach" % (row["p_title"]))
    out("[%] Identifying member edits to talk pages" % (row["p_title"]))
    for ww in members_at:
        for member in members_at[ww]:
            start_date = ww_to_date(ww) + "000000"
            end_date = ww_to_date(ww+1) + "000000"
            query = "SELECT * FROM revision WHERE rev_user_text = %s AND rev_timestamp >= %s AND rev_timestamp < %s"
            rc = rdb.execute(query, (rdb.escape_string(member), start_date, end_date))
            rows = rc.fetchall()
            for row in rows:

    print("\n\n")
    """

    ####
    ## Group awareness - Awareness of content under group scope - Indicated by edits
    ##   to an article that was linked to by a third party on a project page, sub-page,
    ##   template transcluded on either, or corresponding Talk page.
    ####

    out("[%s] Group awareness - awareness of content under group scope" % (row["p_title"]))
    out("[%s] Fetching articles linked to on all project pages" % (row["p_title"]))
    # Distinct from the Group identification query, not limiting to any namespace

    # We'll need to chunk the results of this to avoid tmp table failure, fetch 10000 at a time
    chunk = 0
    chunk_size = 10000
    while True:
        query = "SELECT * FROM coord_project_links WHERE cpl_page_id IN (%s) AND cpl_link_date >= '%s' GROUP BY cpl_link_page_text, cpl_link_date ORDER BY cpl_link_date ASC LIMIT %s,%s" % (",".join(map(str, project_page_ids + project_talk_page_ids)), ww_to_date(min_ww) + "000000", chunk, chunk_size)
        lc = ldb.execute(query)

        if lc.rowcount == 0:
            break
        chunk = chunk + chunk_size

        while True:
            links = lc.fetchmany(1000)
            if not links:
                break

            sys.stdout.write(".")

            pages_edited = {}
            for link in links:
                link_ww = date_to_ww(link["cpl_link_date"])
                edit_weeks = [link_ww, link_ww+1, link_ww+2, link_ww+3]
                #member_ids = merge_dicts(members_at[link_ww], members_at[link_ww+1], members_at[link_ww+2], members_at[link_ww+3]).values()

                members_at_range = {}
                for i in range(4):
                    if link_ww + i in members_at:
                        members_at_range = merge_dicts(members_at_range, members_at[link_ww + i])
                member_ids = members_at_range.values()
                if len(member_ids) == 0:
                    continue

                # For whatever this link links to, any project member that makes a subsequent edit
                # to the linked page indicates awareness.
                query = "SELECT * FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id = %s AND rc_wikiweek IN (%s) GROUP BY rc_user_id, rc_page_id" % (",".join(map(str, member_ids)), link["cpl_link_page_id"], ",".join(map(str, edit_weeks)))
                lc1 = ldb.execute(query)
                edits = lc1.fetchall()
                for edit in edits:
                    edit_date = link["cpl_link_date"]
                    if edit_date not in dependencies:
                        dependencies[edit_date] = []
                    dependencies[edit_date].append("Group awareness")

                    if edit["rc_user_id"] not in dep_by_user:
                        dep_by_user[edit["rc_user_id"]] = {}
                    if edit_date not in dep_by_user[edit["rc_user_id"]]:
                        dep_by_user[edit["rc_user_id"]][edit_date] = []
                    dep_by_user[edit["rc_user_id"]][edit_date].append("Group awareness")

    print("\n\n")

    ####
    ## Cultural awareness - Reciprical edits by project members, limited to User Talk pages. 
    ##   I.e., if member1 AND member2 edit a User Talk page in the same week,
    ##   it indicates a social interaction which may interpreted
    ##   as requiring a level of cultural awareness to successfully participate 
    ##   in the interaction.
    ####

    min_ww = min(members_at.keys())
    max_ww = max(members_at.keys())
    out("[%s] Cultural awareness - successful interaction between project members" % (row["p_title"]))
    out("[%s] Looking for reciprical edits between members between weeks %s and %s" % (row["p_title"], str(min_ww), str(max_ww)))

    this_ww = min_ww
    while this_ww < max_ww:
        if len(members_at[this_ww]) == 0:
            this_ww += 1
            continue

        # Find all user talk page edits by project members for the current week
        query = "SELECT rc_user_id, rc_page_id FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_wikiweek = %s AND rc_page_namespace = 3 GROUP BY rc_user_id, rc_page_id" % ( ",".join(map(str, members_at[this_ww].values())), this_ww)
        lc = ldb.execute(query)
        edits = lc.fetchall()
        # Once we get all the user talk page edits by project members for the current
        # week, determine if there were any two users who both edited the same page
        conversations = {}
        conv_by_user = {}
        for edit in edits:
            if edit["rc_page_id"] not in conversations:
                conversations[edit["rc_page_id"]] = []
            else:
                conversations[edit["rc_page_id"]].append(edit["rc_user_id"])
            

        # Then, for every page that was edited more than once, add a Cultural awareness
        # dependency for this week for that number of edits (i.e., a page that was
        # only edited once, by one user doesn't indicate cultural awareness).
        # (see the GROUP MySQL statement above).
        for page in conversations:
            if len(conversations[page]) > 1:
                edit_date = ww_to_date(this_ww) + "000000"
                if edit_date not in dependencies:
                    dependencies[edit_date] = []
                dependencies[edit_date].append("Cultural awareness")

                for uid in conversations[page]:
                    if uid not in dep_by_user:
                        dep_by_user[uid] = {}
                    if edit_date not in dep_by_user[uid]:
                        dep_by_user[uid][edit_date] = []
                    dep_by_user[uid][edit_date].append("Cultural awareness")


        this_ww += 1

    print("\n\n")

    ####
    ## Social awareness - Social interactions, implied when a project member edits a project Talk
    ##   page for each /project member/ who also edited that page in the last 4 weeks.
    ####

    out("[%s] Social awareness - implicit through exposure to social interactions" % (row["p_title"]))
    out("[%s] Fetching member edits to project talk pages" % (row["p_title"]))

    this_ww = min_ww
    while this_ww < max_ww:
        if len(members_at[this_ww]) == 0:
            this_ww += 1
            continue

        edit_weeks = [link_ww, link_ww-1, link_ww-2, link_ww-3]

        # Grab edits to any project-related Talk pages by all project members
        query = "SELECT rc_user_id, rc_page_id FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s GROUP BY rc_user_id, rc_page_id" % ( ",".join(map(str, members_at[this_ww].values())), ",".join(map(str, project_talk_page_ids)), this_ww)
        lc = ldb.execute(query)
        edits = lc.fetchall()
        # For each edit to a project-related Talk page by project members, determine
        # if any /other project members/ also edited that same page in the prior
        # 4 weeks. For each other member edit, add a Social awareness dependency.
        for edit in edits:
            query = "SELECT rc_user_id, rc_page_id, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_user_id <> %s AND rc_page_id = %s AND rc_wikiweek IN (%s) GROUP BY rc_user_id, rc_wikiweek" % ( ",".join(map(str, members_at[this_ww].values())), edit["rc_user_id"], edit["rc_page_id"], ",".join(map(str, edit_weeks)))
            lc = ldb.execute(query)
            prior_edits = lc.fetchall()
            for prior_edit in prior_edits:
                edit_date = ww_to_date(prior_edit["rc_wikiweek"]) + "000000"
                if edit_date not in dependencies:
                    dependencies[edit_date] = []
                dependencies[edit_date].append("Social awareness")

                if prior_edit["rc_user_id"] not in dep_by_user:
                    dep_by_user[prior_edit["rc_user_id"]] = {}
                if edit_date not in dep_by_user[prior_edit["rc_user_id"]]:
                    dep_by_user[prior_edit["rc_user_id"]][edit_date] = []
                dep_by_user[prior_edit["rc_user_id"]][edit_date].append("Social awareness")

        this_ww += 1

    print("\n\n")

    ####
    ## Coordination activity - Indicated by edits to project pages and sub-pages
    ##   /by project members/, excluding Talk pages.
    ####
    out("[%s] Coordination activity - indicating revisions by members to project related pages." % (row["p_title"]))
    out("[%s] Fetching member edits to project pages (excluding Talk)" % (row["p_title"]))

    this_ww = min_ww
    while this_ww < max_ww:
        if len(members_at[this_ww]) == 0:
            this_ww += 1
            continue

        query = "SELECT rc_user_id, rc_page_id, rc_edits, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s" % (",".join(map(str, members_at[this_ww].values())), ",".join(map(str, project_page_ids)), this_ww)
        lc = ldb.execute(query)
        edits = lc.fetchall()
        for edit in edits:
            # We'll want to add an individual dependency for /each edit/ a project
            # member makes (i.e., for all the rc_edits this member made during
            # the current wikiweek)
            for i in range(edit["rc_edits"]):
                edit_date = ww_to_date(edit["rc_wikiweek"]) + "000000"
                if edit_date not in dependencies:
                    dependencies[edit_date] = []
                dependencies[edit_date].append("Coordination activity")

                if edit["rc_user_id"] not in dep_by_user:
                    dep_by_user[edit["rc_user_id"]] = {}
                if edit_date not in dep_by_user[edit["rc_user_id"]]:
                    dep_by_user[edit["rc_user_id"]][edit_date] = []
                dep_by_user[edit["rc_user_id"]][edit_date].append("Coordination activity")

        this_ww += 1

    print("\n\n")

    ####
    ## Social coordination - Indicated by edits to project Talk pages by project members,
    ##   excluding non-Talk pages.
    ####
    out("[%s] Social coordination - indicated by edits to project Talk pages by members" % (row["p_title"]))
    out("[%s] Fetching member edits to project Talk pages" % (row["p_title"]))

    this_ww = min_ww
    while this_ww < max_ww:
        if len(members_at[this_ww]) == 0:
            this_ww += 1
            continue

        query = "SELECT rc_user_id, rc_page_id, rc_edits, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s" % (",".join(map(str, members_at[this_ww].values())), ",".join(map(str, project_talk_page_ids)), this_ww)
        lc = ldb.execute(query)
        edits = lc.fetchall()
        for edit in edits:
            # Similar to above, we'll want to add dependencies for /each edit/ a project
            # member makes to talk pages.
            for i in range(edit["rc_edits"]):
                edit_date = ww_to_date(edit["rc_wikiweek"]) + "000000"
                if edit_date not in dependencies:
                    dependencies[edit_date] = []
                dependencies[edit_date].append("Social coordination")

                if edit["rc_user_id"] not in dep_by_user:
                    dep_by_user[edit["rc_user_id"]] = {}
                if edit_date not in dep_by_user[edit["rc_user_id"]]:
                    dep_by_user[edit["rc_user_id"]][edit_date] = []
                dep_by_user[edit["rc_user_id"]][edit_date].append("Social coordination")

        this_ww += 1

    print ("\n\n")

    ####
    ## Production activity - Indicated by edits to pages under the scope of a 
    ##   project by group members, excluding Talk pages.
    ####
    out("[%s] Production activity - indicated by edits by project members to project-related articles" % (row["p_title"]))
    out("[%s] Fetching member edits to project-related pages (%s pages)" % (row["p_title"], len(scope_page_ids)))

    this_ww = min_ww
    while this_ww < max_ww:
        if len(members_at[this_ww]) == 0 or len(scope_page_ids) == 0:
            this_ww += 1
            continue

        query = "SELECT rc_user_id, rc_page_id, rc_edits, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s" % (",".join(map(str, members_at[this_ww].values())), ",".join(map(str, scope_page_ids)), this_ww)
        lc = ldb.execute(query)
        edits = lc.fetchall()
        for edit in edits:
            # As above, add dependencies for /each member edit/ to a page under the project's scope
            for i in range(edit["rc_edits"]):
                edit_date = ww_to_date(edit["rc_wikiweek"]) + "000000"
                if edit_date not in dependencies:
                    dependencies[edit_date] = []
                dependencies[edit_date].append("Production activity")

                if edit["rc_user_id"] not in dep_by_user:
                    dep_by_user[edit["rc_user_id"]] = {}
                if edit_date not in dep_by_user[edit["rc_user_id"]]:
                    dep_by_user[edit["rc_user_id"]][edit_date] = []
                dep_by_user[edit["rc_user_id"]][edit_date].append("Production activity")

        this_ww += 1

    print("\n\n")

    ####
    ## Social production - Indicated by edits to Talk pages of articles under the scope
    ##   of a project by group members.
    ####

    out("[%s] Social production - indicated by member edits to Talk pages of project-related articles" % (row["p_title"]))
    out("[%s] Fetching member talk page edits (%s pages) for weeks %s to %s, at: " % (row["p_title"], len(scope_talk_page_ids), min_ww, max_ww))

    this_ww = min_ww
    while this_ww < max_ww:
        sys.stdout.write("%s, " % (this_ww))

        if len(members_at[this_ww]) == 0 or len(scope_talk_page_ids) == 0:
            this_ww += 1
            continue

        for scope_talk_page_id in scope_talk_page_ids:
            #query = "SELECT rc_user_id, rc_page_id, rc_edits, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s" % (",".join(map(str, members_at[this_ww].values())), ",".join(map(str, scope_talk_page_ids)), this_ww)
            query = "SELECT rc_user_id, rc_page_id, rc_edits, rc_wikiweek FROM reflex_cache WHERE rc_user_id IN (%s) AND rc_page_id IN (%s) AND rc_wikiweek = %s" % (",".join(map(str, members_at[this_ww].values())), scope_talk_page_id, this_ww)
            lc = ldb.execute(query)
            edits = lc.fetchall()
            for edit in edits:
                for i in range(edit["rc_edits"]):
                    edit_date = ww_to_date(edit["rc_wikiweek"]) + "000000"
                    if edit_date not in dependencies:
                        dependencies[edit_date] = []
                    dependencies[edit_date].append("Social production")

                    if edit["rc_user_id"] not in dep_by_user:
                        dep_by_user[edit["rc_user_id"]] = {}
                    if edit_date not in dep_by_user[edit["rc_user_id"]]:
                        dep_by_user[edit["rc_user_id"]][edit_date] = []
                    dep_by_user[edit["rc_user_id"]][edit_date].append("Social production")

        this_ww += 1


    # Once we're done, save the dependency data in a .csv file
    csv = "dependency_trajectories_2_%s.csv" % (row["p_title"])
    f = codecs.open(csv, "w", encoding="utf-8")

    # Make sure we're printing dates chronologically
    dates = dependencies.keys()
    dates.sort()
    for date in dates:
        for dependency in dependencies[date]:
            f.write(str(date) + "," + dependency + "\n")
    f.close()

    pickle_struc(f="dep_by_user_%s.dat" % (row["p_title"]), d=dep_by_user)

    # Also, print out the dependency trajectory for the top 3 users for this project
    top_users = {}
    for uid in dep_by_user:
        top_users[uid] = 0
        for date in dep_by_user[uid]:
            for dep in dep_by_user[uid][date]:
                top_users[uid] += 1
    # Then, grab the top users from top_users (value)
    sorted_top_users = sorted(top_users.items(), key=operator.itemgetter(1))[::-1][:3]
    for u in sorted_top_users:
        uid = u[0]
        csv = "dependency_trajectories_2_%s_%s.csv" % (uid, row["p_title"])
        f = codecs.open(csv, "w", encoding="utf-8")

        # Make sure we're printing dates chronologically
        dates = dep_by_user[uid].keys()
        dates.sort()
        for date in dates:
            for dependency in dependencies[date]:
                f.write(str(date) + "," + dependency + "\n")
        f.close()


def getMemberId(member):
    query = "SELECT tu_id FROM ts_users WHERE tu_name = %s"
    lc = ldb.execute(query, member)
    row = lc.fetchone()
    if not row:
        return 0
    else:
        return row["tu_id"]

# From http://stackoverflow.com/questions/38987/how-can-i-merge-two-python-dictionaries-in-a-single-expression
def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def main():
    # First, grab the page ids of all projects, and all corresponding talk pages
    query = "SELECT * FROM project JOIN ts_pages ON p_title = tp_title WHERE tp_namespace IN (4,5)"
    lc = ldb.execute(query)
    rows = lc.fetchall()
    for row in rows:
        if row["tp_namespace"] == 4:
            all_project_page_ids.append(row["p_id"])
        else:
            all_project_talk_page_ids.append(row["p_id"])

    # Then, grab the project ids we're interested in
    query = 'SELECT * FROM project WHERE p_title IN ("WikiProject_Feminism", "WikiProject_Piracy", "WikiProject_Medicine", "WikiProject_Plants", "WikiProject_Chemistry", "WikiProject_Spoken_Wikipedia", "WikiProject_Countering_systemic_bias", "WikiProject_Copyright_Cleanup", "WikiProject_Missing_encyclopedic_articles", "WikiProject_Outreach")'
    lc = ldb.execute(query)
    rows = lc.fetchall()
    for row in rows:
        #if row["p_title"] != "WikiProject_Copyright_Cleanup":
        if row["p_title"] != sys.argv[1]:
            continue

        #print(sys.argv[1])
        #sys.exit(0)

        getCoordinationEdits(row)


if __name__ == "__main__":
    main()


