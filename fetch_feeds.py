from lxml.html import parse
from lxml import etree
from multiprocessing import Process, cpu_count
from progress_bar import ProgressBar
from config import config
import urllib2
import pdb
import re
import os
import sys
import time
import socket
import platform
import redis
import cPickle
import glob
import logging

"""
fetch_feeds.py

The functions in this library extract the base URL for each blog and
attempt to autodiscover the corred blog feed URL (RSS), and scrape
the content of each feed (in parallel) and write to disk. extract_titles
extracts the title of each blog post from the feed from disk.
extract_titles should be in another file, but shoved it here for space.
"""
#TO DO:
#1. CHANGE THE USER AGENT!
#2. SPLIT INTO MORE FUNCTIONS.
#3. Blog URLs can be kept in memory and returned, or written to disk.

patt = re.compile(r'(.*?) (?:/ (.*?) )?blogs')

prefix = config['PREFIX']
feeds_path = prefix + "raw_feeds/"

logging.basicConfig(filename='%s/logs/fetchfeeds-py_%s.log' % (config.config['PREFIX'], platform.node()),
    format='%(levelname)s: %(asctime)s %(message)s', level=logging.INFO)

HTTP_RETRIES = 3      # Number of times to retry on HTTP or connection failure.
HTTP_TIMEOUT = 7      # Number of seconds to wait for a response if delay.
socket.setdefaulttimeout(HTTP_TIMEOUT)  # Fail if we wait too long.


def publish_to_redis(urls):
    #Initialize work queue.
    r = redis.Redis()
    r.flushall()
    r.set('errors', '0')
    r.set('successes', '0')

    for url in urls:
        r.rpush('blogs', url)


def harvest_urls():
    """Walk through all of the pages for each category and extract the base URL
    for each blog.

    Returns: none.

    Side effect:  Raw HTML for each page of each category written to disk and a
    blog -> cat mapping to disk.
    """
    manifest = []
    category = {}
    subcategory = {}
    directoryfiles = "/tmp/technorati/directory_listing/"
    # ^^ the directory containing the HTML from the Technorati site.

    #Set up directory for intermediate data: MANIFEST
    #MANIFEST contains: Category, Subcategory, Title and URL.
    #and is a roster of URLs of blogs to autodiscover.
    if not os.path.exists(prefix + "meta"):
        os.mkdir(prefix + "meta")
    else:
      #TO DO: What if meta exists but MANIFEST got deleted?
        logging.info("Blog URLs already harvested. Skipping...")
        return

    #Iterate through each file in the directory and extract blog URLs.
    for infile in glob.glob(os.path.join(directoryfiles, '*.html')):
        logging.info("Harvesting blog URLs from %s." % infile)
        dirpage = file(infile)
        root = parse(dirpage).getroot()
        #Rather than infer the category from the filename, just extract
        #it from the file. Not the best way to do this, hit is minimal.
	pieces = infile.split('/')[-1].split('_')
	cat = pieces[1]
	subcat = None
	if len(pieces) == 4:
		subcat = pieces[2]
        blogs = root.xpath("//td[@class='site-details']")
        #Iterate through all of the blogs listed on the page.
        for blog in blogs:
            url = blog.xpath("a[@class='offsite']")[0].text
            title = blog.xpath('h3/a')[0].text
            OUT = open(prefix + "meta/MANIFEST", "a")
            #Store the category of the blog.
            category[url] = cat
            if subcat:
                output = [cat, subcat, title.encode('utf-8').replace('    ', ' '), url]
                subcategory[url] = subcat
                print >> OUT, '    '.join(output)
            else:
                output = [cat, "NA", title.encode('utf-8').replace('    ', ' '), url]
                print >> OUT, '\t'.join(output)
            manifest.append(output)
        OUT.close()
    # This is a hack to get around having to use a database.
    # TODO: Reimplement using a database.
    BLOGCATS = open(prefix + "blogcats.pickle", "w")
    cPickle.dump(category, BLOGCATS)
    BLOGCATS.close()
    return manifest

"""
The PROBE trifecta. All perform similar function.
probe:        given a blog URL, "probe", or autodiscover the feed URL.
probe_feeds:  drives the probing process in parallel. A "main" function.
probe_worker: processes Redis queue, monitors progress, calles probe.
probe_feeds calls probe_worker calls probe
"""


def probe(urlbase, fileno, ip):
    """Attempts to autodiscover the RSS feed URL given a base url
    download the feed to disk.

    urlbase    the URL containing the links to the RSS feeds.
    fileno     a sequence identifier. The page number read by the process.

    Returns True on success, False otherwise.

    Side effects: 1. raw feeds dumped to disk
                  2. files containing mapping from PID+number to URL.
    """

    r = redis.Redis(ip)
    #FETCH
    cur = 0
    """
    This process is error prone. Try several times to fetch a website.
    Protection against a few different errors:
    - one-off connection or HTTP errors.
    - crawler traps, where the site hangs and does not return response.
    - all other errors are fatal, and the site is skipped.
    """
    # Fetch the website containing the feed.
    while cur < HTTP_RETRIES:
        try:
            page = urllib2.urlopen(urlbase)
            break
        except urllib2.URLError:
            #This exception likely means the server hanged in giving a response.
            TRAP = open(prefix + "meta/access_traps-%s.log" % str(os.getpid()), "a")
            print >> TRAP, urlbase
            TRAP.close()
        cur += 1
    if cur == HTTP_RETRIES:  # timeout on all tries.
        raise Exception("Exceeded HTTP_RETRIES without success.")

    # Parse the HTML for the website to find the RSS feed links.
    root = parse(page).getroot()
    #Use the autodiscovery standard, ignoring comment feeds.
    links = root.xpath('//link[@type="application/rss+xml" and ' +
                       'not(contains(@title, "comment")) and not(contains(@href, "comment"))]')
    if len(links) > 0:
        #at least one feed was found.
        OUT = open(prefix + "meta/access_ready-%s.log" % str(os.getpid()), "a")
        url = links[0].get('href')
        #Just take the first URL, and convert it to an absolute link.
        if not url.startswith('http://') and not url.startswith('feed://') and \
           not url.startswith('https://'):
            url = urlbase + url
            print >> OUT, urlbase, url, links[0].get('title').encode('utf-8')
            OUT.close()
            r.rpush('good_url', url)
        else:
            #1) Webmaster chose not to abide by the standard
            #2) Webmaster provided bad URL that does not contain feed links.
            NOLINK = open(prefix + "meta/access_nolink-%s.log"
                          % str(os.getpid()), "a")   # No link found.
            print >> NOLINK, urlbase
            NOLINK.close()
            raise Exception("No URLs detected.")
            return False

    # Now fetch the actual RSS feed.
    cur = 0
    while cur < HTTP_RETRIES:
        cur += 1
        try:
            resp = urllib2.urlopen(url)
            content = resp.read()
            RAW = open(feeds_path + "%s-%s.xml" % (str(os.getpid()), str(fileno)), "w")
            try:
                #DUMP THE FEED TO DISK
                print >> RAW, content
            except UnicodeError:
                print >> RAW, content.encode('utf-8')
            RAW.close()
            #MAP THIS FEED URL TO A PID AND CURSOR.
            KEY = open(feeds_path + "key-%s.dat" % os.getpid(), "a")
            print >> KEY, ' '.join([url, str(fileno)])
            KEY.close()
            break
        except Exception, e:
            #Just tell the user and try again.
            logging.info("Exception occurred fetching %s: %s" % (url, str(e)))
            pass
        time.sleep(1)
        break
    else:
        FAIL = open(prefix + "meta/failed-%s.log" % str(os.getpid()), "a")
        print >> FAIL, url
        FAIL.close()
        raise Exception("RSS feed found but could not be fetched.")
    return True


def probe_feeds(master_ip, cores=cpu_count(), distributed=False):
    """Autodiscover RSS feeds from a manifest, in parallel.

    Input is either a list of URLs, or an IP address to a Redis
    server containing the list of URLs.

    manifest    a list of URLs of blogs.

    Returns
    """

    logging.info("Fetching feeds using HTTP on %d cores." % cores)
    cores = 1
    probe_worker(master_ip)
    #Setup processes for parallel processing and the progress bar.
    #procs = [Process(target=probe_worker, args=(master_ip,)) for p in range(cores-1)]
    # Only launch progress bar if local.
    #if not distributed:
    #    procs.append(Process(target=progbar, args=(master_ip,)))
    #for p in procs:
    #    p.start()
    #for p in procs:
    #    p.join()
    r = redis.Redis(master_ip)
    #pdb.set_trace()
    #logging.info("Success so far: %d" % int(r.get('successes')))
    #logging.info("Failures so far: %d" % int(r.get('errors')))
    #logging.info("Probing completed for this worker.")


def progbar(ip):
    r = redis.Redis(ip)
    initial = r.llen('blogs')
    prog = ProgressBar(0, initial, 77, mode='fixed', char='#')
    while True:
        prog.update_amount(initial - r.llen('blogs'))
        print prog, '\r',
        sys.stdout.flush()
        if initial - r.llen('blogs') == initial:
            break
        time.sleep(1)
    return


def probe_worker(ip):
    """Utility that simply pulls a URL off the queue and makes
    a call to "probe" to actually do the work.

    foo   Dummy argument. Just to shutup the job constructor.
    """
    fileno = 0
    r = redis.Redis(ip)
    while True:
        pdb.set_trace()
        url = r.lpop('blogs')   # pull URL off queue.
        if not url:
            break     # means we are done.
        try:
            content = probe(url, fileno, ip)    # does the work.
            if content:
                r.incr('successes')
            else:
                r.incr('errors')
        except Exception, e:
            # TODO(ryan): What kind of error occurred?
            r.incr('errors')
            logging.info("Error occured: %s, URL: %s" % (e, url))
            ERR = open(prefix + "meta/errors-%s.log" % str(os.getpid()), "a")
            print >> ERR, url, e
            ERR.close()
            r.rpush('errors', '|'.join([url, e]))
        fileno += 1


def extract_titles():
    """Utility that simply pulls a URL off the queue and makes
    a call to "probe" to actually do the work.

    foo   Dummy argument. Just to shutup the job constructor.
    """
    """
    The final data has the mapping post_title -> cat.
    This requires three relations:
    (pid, id) -> feed_url, feed_url -> blog_url, blog_url -> cat.
    Each file contains one raw feed with several titles, thus:
    (pid, id) -> list(post_title, cat)
    """
    #(pid, id) -> feed_url
    idvals = cPickle.load(open(prefix + "idvals.pickle"))
    #blog_url -> cat
    cats = cPickle.load(open(prefix + "blogcats.pickle"))
    #feed_url -> blog_url
    urls = cPickle.load(open(prefix + "blogurls.pickle"))

    patt = re.compile('<title>(.*?)</title>')
    titles_success = 0
    titles_bad = 0
    successes = 0
    failures = 0
    #iterate through all raw feed HTML files.
    for infile in glob.glob(os.path.join(feeds_path, '*.xml')):
        info = infile.split('.')[0].split('/')[-1]
        pid, id = info.split('-')
        #(pid, id) -> blog
        blog = idvals[(int(pid), int(id))]
        cat = None
        try:
            # blog -> url -> cat
            cat = cats[urls[blog]]
        except KeyError:
            logging.info("Could not find category for blog %s. Skipping..." % blog)
            continue
        try:
            root = etree.parse(infile)
            successes += 1
        except Exception:
            logging.info("Title extraction failed for %s." % infile)
            failures += 1
            continue

    #PARSE THE FILE
    #Get the encoding of the document (doesn't seem to work)
    enc = root.docinfo.encoding
    titles = root.xpath('/rss/channel/item/title')  # titles should be here.
    OUT = open(prefix + "meta/titles.dat", "a")
    if len(titles) == 0:    # didn't find titles using that xpath.
        IN = open(infile)     # look for the title in HTML instead.
        content = IN.read()
        IN.close()
        titles = patt.findall(content)
        #for each found title, print it to the FINAL log used for research.
        for title in titles:
            if title is not None:
                try:
                    print >> OUT, ','.join([blog, cat, str(info),
                                  title.strip().replace(",", "")])
                    titles_success += 1
                except:
                    try:
                        print >> ','.join([OUT, blog, cat, str(info),
                                          title.strip().encode(enc).replace(",", "")])
                        titles_success += 1
                    except:
                        titles_bad += 1
                        logging.info("Character encoding failed in file %s." % infile)
            else:
                titles_bad += 1
    else:
        for title in titles:
            if title.text is None:
                titles_bad += 1
                continue
            try:
                print >> OUT, ','.join([blog, cat, str(info),
                              title.text.strip().encode(enc).replace(",", "")])
                titles_success += 1
            except:
                logging.info("Character encoding failed in file %s." % infile)
                titles_bad += 1
        OUT.close()
    logging.info("Document Parse Successes: %d" % successes)
    logging.info("Document Parse Failes Failures:  %d" % failures)
    logging.info("TOTAL TITLES FETCHED: %d (%d failed)" %
                (titles_success, titles_bad))


def reconcile():
    """Utility that combines all of the PID-ID mappings and URLs into one data
    structure. This is a hack and there is a better way to do this using a DB.

    Side effects: writes index (pid, id) -> url and feed_url -> blog_url
    mappings to disk
    """
    idvals = {}
    urls = {}
    # patt = re.compile(r'([0-9]+)')
    #Each URL is uniquely identified by a PID and an ID number.
    for infile in glob.glob(os.path.join(feeds_path + "key*")):
        KEY = open(infile)
        pid = infile.split('-')[1].split('.')[0]
        for line in KEY:
            temp = line.strip().split(' ')
            if len(temp) > 2:     # HACK
                url = temp[0]
                idval = temp[2]
            else:
                url, idval = temp
            idvals[(int(pid), int(idval))] = url  # (pid, id) -> url
        KEY.close()
    #Every feed URL is associated with a blog URL.
    for infile in glob.glob(os.path.join(prefix + "meta/access_ready*")):
        IN = open(infile)
        for line in IN:
            blog, url = line.strip().split(' ')[0:2]
            urls[url] = blog
        IN.close()
    #Dump these mappings to disk.
    cPickle.dump(idvals, open(prefix + "idvals.pickle", "w"))
    cPickle.dump(urls, open(prefix + "blogurls.pickle", "w"))

'''
Graveyard

    # Either the list of URLs is passed in, is stored on disk,
    # or is distributed in Redis.
    if not kwargs['urls']:  # not passed in.
        # If in local mode, use file.
        IN = open(prefix + "meta/MANIFEST")
        for line in IN:
            cat, title, url = line.strip().split('    ')
            r.rpush('blogs', url)
        IN.close()
    else:  # URLs were passed in.
        for blog in kwargs['urls']:
            r.push('blogs', blog[3])  # Insert URLs into work queue.

        if not os.path.exists(feeds_path):
        os.mkdir(feeds_path)
    if not manifest:
        logging.info("Feeds already downloaded. Skipping...")
        return

'''