#!/usr/bin/env python
# collect-sosp-data.py
# @Author:      Diwaker Gupta (diwaker@floatingsun.net)
# @Last Change: 2008-02-27

# standard library
import hashlib
import logging
import optparse
import os
import re
import sqlite3
import sys
import urllib2

# non-standard libraries
import html5lib

from html5lib import treebuilders

parser = optparse.OptionParser("%prog [options] args")
parser.add_option("--verbose", "-v", action = "store_true", dest = "verbose",
                  default = False, help = "Increase verbosity.")
parser.add_option("--debug", "-d", action = "store_true", dest = "debug",
                  default = False, help = "Debug mode.")
(options, args) = parser.parse_args()

kAcmPortal = "http://portal.acm.org"

def getSoupFromURL(url):
  page = urllib2.urlopen(url)
  parser = html5lib.HTMLParser(tree =
                               treebuilders.getTreeBuilder("beautifulsoup"))
  return parser.parse(page)

def main(options, args):
  log = logging.getLogger("collect-acm-data")
  console = logging.StreamHandler()
  log.addHandler(console)
  if options.debug:
    log.setLevel(logging.DEBUG)
  else:
    log.setLevel(logging.INFO)

  log.info("Creating database")
  conn = sqlite3.connect("acm.db")
  db = conn.cursor()

  log.debug("Creating tables")
  db.execute("""
          create table if not exists conferences (
          nick text primary key not null,
          fullname text)
          """)

  db.execute("""
          create table if not exists papers (
          id text primary key not null,
          cid text,
          year integer,
          title text)
          """)

  db.execute("""
          create table if not exists keywords (
          id text primary key not null,
          name text)
          """)

  db.execute("""
          create table if not exists keymap (
          pid text not null,
          kid text not null,
          primary key (pid, kid))
          """)

  conferences = {
    "sosp":("Symposium on Operating System Principles",
            "http://portal.acm.org/toc.cfm?id=SERIES372&idx=SERIES372&" +
            "type=series&coll=portal&dl=ACM&part=series&WantType=Proceedings&" +
            "title=SOSP&CFID=18013301&CFTOKEN=14837976"),
    "sigcomm":("SIGCOMM",
               "http://portal.acm.org/toc.cfm?id=SERIES419&idx=SERIES419&" +
               "type=series&coll=portal&dl=ACM&part=series&WantType=Proceedings&"+
               "title=COMM&CFID=18013301&CFTOKEN=14837976"), }

  for conf in conferences.keys():
    if options.verbose:
      log.info("Processing %s" % conf)

    if conf == "sigcomm":
      continue

    try:
      db.execute("insert into conferences values (?,?)",
                 (conf, conferences[conf][0]))
    except:
      pass

    # Get the page for each conference
    log.debug("Reading %s" % conferences[conf][1])
    soup = getSoupFromURL(conferences[conf][1])

    archives = []
    for t in soup.body("a"):
      if conf == "sosp" and len(t.contents) > 0:
        text = str(t.contents[0])
        if text and text.startswith(" Proceedings of"):
          log.debug(t)
          archives.append(t)
      elif conf == "sigcomm" and len(t.contents) > 0:
        text = str(t.contents[0])
        if text and text.startswith(" Proceedings of the"):
          archives.append(t)

    # For each year, get a list of all the papers
    pdfs = []
    papers = []

    if options.debug:
      archives = archives[0:1]

    for a in archives:
      url = "%s/%s" % (kAcmPortal, a["href"])
      log.debug("Opening %s" % url)
      soup = getSoupFromURL(url)
      for t in soup.body("a", href = re.compile("^citation.*type=series.*WantType=Proceedings")):
        log.debug("Found paper %s" % t)
        papers.append(t["href"])
      for t in soup.body("a", href=re.compile("type=pdf")):
        log.debug("Found paper %s" % t)
        pdfs.append(t["href"])

    if options.debug:
      papers = papers[0:2]
    for p in papers:
      url = "%s/%s" % (kAcmPortal, p)
      log.debug("Opening %s" % url)
      soup = getSoupFromURL(url)

      pid = re.search("id=(\d+\.\d+)", p).group(1)
      title = soup.find("td", attrs={"class":"medium-text", "colspan":"3"})
      year = soup.find(text=re.compile("Year of Publication:"))
      log.debug(year)
      year = int(year.strip().split(":")[1])
      vals = []
      log.debug(title.contents)
      for t in title.contents:
        vals.append(unicode(t.contents[0]))
      log.debug(vals)
      title = ("".join(vals)).strip()
      log.info("Processing %s" % title)
      try:
        db.execute("insert into papers values (?,?,?,?)",
                   (pid, conf, year, title))
      except:
        pass
      tag_queries = ["query=PrimaryCCS",
                     "query=CCS",
                     "query=Subject",
                     "query=General Terms",
                     "query=Keywords"]
      for q in tag_queries:
        for c in soup.body("a", href=re.compile(q)):
          tag = c.contents[0].strip().lower()
          tid = hashlib.sha256(tag).hexdigest()
          log.debug("%s: %s" % (tag, tid))
          log.info("%d:%s:%s" % (year, title, tag))
          # If the keyword already exists, the first try block will fail, but
          # the keymap should still get updated correctly.
          try:
            db.execute("insert into keywords values (?,?)", (tid, tag))
          except:
            pass
          try:
            db.execute("insert into keymap values (?,?)", (pid, tid))
          except:
            pass

  conn.commit()
  db.close()

if __name__ == "__main__":
  options, args = parser.parse_args()
  main(options, args)
