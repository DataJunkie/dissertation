#!/usr/bin/env python

from lxml.html import parse
from StringIO import StringIO
import urllib2
import sys
import re
import time
import cPickle
import os
import pdb
import logging

"""
technorati.py

The functions in this library perform all of the necessary interactions with
the technorati.org website including discovering blog categories and
subcategories as well as crawling each category and writing the *listings*
to disk.
"""

logging.basicConfig(format='%(levelname)s: %(asctime)s %(message)s', level=logging.INFO)

"""
TO DO:
1. Allow user to specify prefix directory.
2. Allow user to specify either database I/O and/or file writing.
"""

prefix = "/tmp/technorati/"  # root directory for temo data for project. See TODO #1.
pickle = True        # Write taxonomy as a pickle?
text = True          # Write taxonomy as a flat file?

HTTP_MAX_PAGE = 20  # Maximum number of pages to scrape per category. Avoid banning.


def get_directory():
    """Extracts the category/subcategory hierarchy from the Technorati main site

    No arguments.

    Returns: Taxonomy as a dict of lists.

    Side effect:  Categories/subcategory hierarchy written to disk.
    """
    logging.info('Starting indexing at technorati.com.')

    base = "http://technorati.com/blogs/directory/"

    logging.info("Discovering Technorati blog categories...")

    #Prepare output directory.
    if not os.path.exists(prefix + "directory_listing"):
        os.mkdir(prefix)
        os.mkdir(prefix + "directory_listing")
    else:
        # If taxonomy already exists, open it from disk and return it.
        logging.info('Taxonomy info already exists. Skipping...')
        with open(prefix + "taxonomy.pickle") as taxonomy:
            cats = cPickle.load(taxonomy)
        # BUG: What if pickle is False?
        # BUG: What if the the directory exists but taxonomy is not?
        return cats

    #Try to fetch the page. Since this is the first step, if it fails,
    #just let the whole process fail.
    text = ""
    try:
        text = urllib2.urlopen(base).read()
    except:
        logging.error('Cannot fetch main Technorati page. Process failed.')
        sys.exit(-1)
    root = parse(StringIO(text)).getroot()
    #XPath to extract the DOM element corresponding to each category.
    cat_tags = root.xpath("//div[@class='round-rect-green ' \
                        or @class='round-rect-green no-content']\
                        /div[@class='inner']")

    #categories: (category, url) -> list( (subcategory, url) )
    #categories is a dict of lists and stores the taxonomy.
    categories = {}
    for cat in cat_tags:
      #each element of the DOM assoc with a category, get the link.
        node = cat.xpath("./h2/a")
        name, url = node[0].text, node[0].get('href')
        categories[(name, url)] = []
        #Get the list containing the subcategories for category cat.
        subcat_tags = cat.xpath('./ul/li/a')
        for subcat in subcat_tags:
            #Get the name and URL and add to the taxonomy.
            subname = re.search('([A-Z][A-Za-z./ ]+)', subcat.text_content()).group()
            suburl = subcat.get('href')
            categories[(name, url)].append((subname, suburl))
    logging.info("Done.")
    logging.info("Writing taxonomy to disk...")
    if pickle:
        OUT = open(prefix + "taxonomy.pickle", "w")
        cPickle.dump(categories, OUT)
        OUT.close()
    if text:
        #Print to flat file:
        #taxonomy_categories: one category name per line.
        #taxonomy_subcategories: category subcategory
        with open(prefix + "taxonomy_categories.dat", "w") as OUT:
            for key in categories:
                OUT.write(key[0] + '\n')
        with open(prefix + "taxonomy_subcategories.dat", "w") as OUT:
            for key in categories:
                for value in categories[key]:
                    OUT.write(key[0] + '|' + value[0] + '\n')
        return categories


def get_directory_listings(taxonomy):
    """Scrapes each category and each page in each category has its own file.

    taxonomy - a dict of lists; hierarchy of taxonomy (assumed valid)

    Returns: none.

    Side effect:  Raw HTML for each page of each category written to disk.
    """
    if len(os.listdir(prefix + "directory_listing/")) > 0:
        logging.info("Already downloaded directory listing. Skipping...")
        return
    for cat in taxonomy:
        #each category, scrape each page.
        logging.info("Scraping category %s." % cat[0])
        url = cat[1]
        pg = 1  # page counter. Set upper maximum to avoid being banned.
        while True:
            toOpen = "http://technorati.com" + url + "page-%s/" % str(pg)
            # technorati.org is fairly stable. Assume the page fetched without error.
            # If not, FAIL the whole process. This is very rare.
            try:
                page = urllib2.urlopen(toOpen).read().replace('\n', '')
                if not page:
                    raise Exception()
            except:
                logging.error("Could not fetch page %d of category %s. FAIL." % (pg, cat[0]))
                sys.exit(-1)
            if pg == 1:
                # Extract the last page number.
                last_page = re.compile('</span><a href="%s' % url + 'page-([0-9]+)/">')
                max_page = last_page.search(page.replace('\n', '')).group(1)
            # Dump to disk.
            with open(prefix + "directory_listing/tech_%s_%s.html" % (url.split('/')[-2], str(pg)), "w") as OUT:
                print prefix + "directory_listing/tech_%s_%s.html" % (url.split('/')[-2], str(pg))
                OUT.write(page + '\n')
            pg += 1
            # Technorati tends to ban for hammering. Just stop after 20 pages.
            if pg > min(max_page, HTTP_MAX_PAGE):
                break
            # Let the server recover after our abuse.
            time.sleep(1)
    logging.info("Done.")
    for cat in taxonomy:
        #each category, scrape each page.
        logging.info("Scraping subcategories of category %s." % cat[0])
        subcats = taxonomy[cat]
        if subcats:
            for subcat in subcats:
                url = subcat[1]
                pg = 1  # page counter. Set upper maximum to avoid being banned.
                while True:
                    toOpen = "http://technorati.com" + url + "page-%s/" % str(pg)
                    # technorati.org is fairly stable. Assume the page fetched without error.
                    # If not, FAIL the whole process. This is very rare.
                    try:
                        page = urllib2.urlopen(toOpen).read().replace('\n', '')
                        if not page:
                            raise Exception()
                    except Exception as e:
                        logging.error("Could not fetch page %d of subcategory %s. %s FAIL." % (pg, cat[0], e))
                        sys.exit(-1)
                    if pg == 1:
                        # Extract the last page number.
                        last_page = re.compile('</span><a href="%s' % url + 'page-([0-9]+)/">')
                        max_page = last_page.search(page.replace('\n', '')).group(1)
                    # Dump to disk.
                    with open(prefix + "directory_listing/tech_%s_%s_%s.html" %
                        (url.split('/')[3], url.split('/')[4], str(pg)), "w") \
                        as OUT:
        			        OUT.write(page + '\n')
                    pg += 1
                    # Technorati tends to ban for hammering. Just stop after 20 pages.
                    if pg > min(max_page, HTTP_MAX_PAGE):
                      break
                    # Let the server recover after our abuse.
                    time.sleep(1)
