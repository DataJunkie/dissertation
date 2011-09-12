import technorati
import config
import fetch_feeds
import sanitizing_modeling

"""
TO DO:
1. Add switches to control what executes and what does not.
2. Divide workflow into more functions.
3. Use database JOINs rather than having to reconcile multiple dicts.
"""

tax = technorati.get_directory()
technorati.get_directory_listings(tax)
blogurls = fetch_feeds.harvest_urls()
fetch_feeds.probe_feeds(blogurls)
fetch_feeds.reconcile()
fetch_feeds.extract_titles()
sanitizing_modeling.sanitize("/home/ryan/Documents/STOPWORDS", "/home/ryan/dissertation/technorati_docs.csv")
