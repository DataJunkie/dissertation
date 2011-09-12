#!/usr/bin/env python

import sys
import pdb
import os
from collections import defaultdict
from config import prefix
import logging

"""
sanitizing_modeling.py

The functions in this library parse a roster of blog post titles and
prepare the text for analysis.
"""

lexicon_file = "meta/LEXICON"     #contains each word in the lexicon,1 per line.
titles_file = "meta/titles.dat"   #contains blog url, category, pid/id, 
                                  #title
dist_file = "meta/DISTRIBUTION"   #contains pid/id (document ID) and a word.
                                  #One line per word/doc pair.

#Remove all non-alphas from text.
delchars = ''.join(c for c in map(chr, range(256)) if not c.isalpha())

def sanitize(stopwords_file, analysis_path):
  """Performs various text mining and sanitizing functions including removing
  stop words, removing infrequent words, removing non-word characters, 
  converting all letters to lowercase. 

  stopwords_file    path to file containing stopwords to remove, one per line.
  analysis_path     path to file containing blog post titles, one per line:
                    category/label\ttitle text
  """
  #Populate stopwords dictionary.
  stop = []
  IN = open(stopwords_file)
  for line in IN:
    stop.append(line.strip())
  IN.close()

  #Load the LEXICON from disk, or create it.
  #During creation, load all words, remove non-alpha, ignore stopwords,
  #and include if word appears more than 20 times in corpus.
  if not os.path.exists(prefix + lexicon_file):
    logging.info("No LEXICON file found. Building...")
    wordcount = defaultdict(int)
    lexicon = {}
    IN = open(prefix + titles_file)
    for line in IN:
      tokens = line.strip().split(',')
      if len(tokens) != 4:  #Bad data.
        continue
      tokens = tokens[3].split(' ') #extract title
      for token in tokens:
        allalpha = token.translate(None, delchars)
        if len(allalpha) > 0 and allalpha.lower() not in stop:
          wordcount[allalpha.lower()] += 1
    IN.close()
    #Only retain words that appear more than 20 times.
    LEXICON = open(prefix + lexicon_file, "w")
    logging.info("Removing infrequent words from lexicon...")
    for word in wordcount:
      if wordcount[word] >= 20:
        lexicon[word] = True
        print >> LEXICON, word
    LEXICON.close()
  else:
    #Read lexicon file into memory.
    logging.info("Found LEXICON file. Loading.")
    LEXICON = open(prefix + lexicon_file)
    lexicon = {}
    for line in LEXICON:
      lexicon[line.strip()] = True
    LEXICON.close()
  #TO DO: Get blog and category tp merge with

  #Create FINAL file for topic modeling. 
  IN = open(prefix + titles_file)
  OUT = open(prefix + dist_file, "w")
  DOCS = open(analysis_path, "w")
  logging.info("Sanitizing text into final form for analysis.")
  skipped = 0
  docno = 0
  for line in IN:
    try:
      blog, cat, docid, title = line.strip().split(',')
    except:
      skipped += 1
      continue
    title_tokens = title.split(' ')
    trimmed = []
    for token in title_tokens:
      #Check each word against the lexicon. Keep if in there. Dump o/w.
      allalpha = token.translate(None, delchars)
      if len(allalpha) > 0 and allalpha.lower() not in stop:
        if len(lexicon) > 0 and allalpha.lower() in lexicon:
          trimmed.append(token.translate(None, delchars))
    if len(trimmed) > 5:
      #If the document is long enough, print each word/document pair, 
      #otherwise ignore this document.
      for t in trimmed:
        print >> OUT, ','.join([blog, cat, str(docid) + "-" + 
          str(docno), t.lower()])
      print >> DOCS, ','.join([cat, ' '.join([t.lower() for t in trimmed])])
    docno += 1
  print "Skipped %d" % skipped
  IN.close()
  OUT.close()
  DOCS.close()
