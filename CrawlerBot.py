#!/usr/bin/env python
"""
CrawlerBot

A website crawler which can store scraped links in a SQLite database
or generate an XML sitemap.

Copyright (c) 2015 Sam Saint-Pettersen.
Released under the MIT/X11 License.

Use -h switch for usage information.
"""
import sys
import os
import re
import time
import codecs
import argparse
import datetime
import urllib2
import sqlite3
from HTMLParser import HTMLParser
import xml.etree.ElementTree as ET
import xml.dom.minidom as xml

# This class handles the crawling itself.
class CrawlerBot:

	disallowed = [] # Disallowed links.
	links = [] # Allowed links.
	titles = [] # Titles for links.
	descs = [] # Descriptions for links.
	keywords = [] # Keywords for links.
	locs = [] # Links for keyworded sitemap.
	sitemaps = [] # Links for found sitemaps.

	nofollow = False # Do not follow a link.
	verbose = False # Not verbose by default.
	daemon = False # Not a daemon by default.
	keyworded = False # Do not generate keyworded sitemap by default.
	sites = []
	types = []
	outs = []
	is_title = False
	robots_str = 'CrawlerBot' # User agent string as used in a robots.txt user agent directive.
	ua_str = 'Mozilla/5.0 (compatible; CrawlerBot/1.0; +https://github.com/stpettersens/CrawlerBot)' 
	headers = { 'User-Agent': ua_str }
	# UA string as used in HTTP requests.
	website = '' # Website to crawl; will be overriden on invocation.
	current = '' # Current page to crawl; will be overriden on invocation.

	cache = 'cache.db' # Filename to use for caching database.

	# Cache link to SQLite database file.
	@staticmethod
	def cacheToDatabase(link):
		conn = sqlite3.connect(CrawlerBot.cache)
		c = conn.cursor()
		c.execute('''CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, link VARCHAR(50))''') # Create table.
		c.execute("INSERT INTO links (link) VALUES ('{0}')".format(link)) # Insert a row of data.
		# Save (commit) the changes.
		conn.commit()

		# Close the connection.
		conn.close()

	def __init__(self, crawl_file, site, out, db, sitemap, keyworded, verbose, version, info, daemon, interval):
		if len(sys.argv) == 1 or info: 
			print(__doc__) # Display program information.
		elif version:
			print(CrawlerBot.ua_str) # Print user-agent string, which contains version (1.0).
		elif file != None:
			self.loadCrawlJobs(crawl_file)
		
		if site == None: site = ''
		if verbose == True: CrawlerBot.verbose = True # Set to be verbose.
		if daemon:
			if interval == None: interval = 7200 # Defaults to 7200 seconds or a 2 hour interval.
			self.runAsDaemon(site, out, db, keyworded, sitemap, interval)
		elif len(CrawlerBot.sites) > 0:
			self.doCrawls()
		else:
			self.doCrawl(site, out, db, keyworded, sitemap)

	# Load crawl jobs from file
	def loadCrawlJobs(self, crawl_file):
		tree = ET.parse(crawl_file)
		root = tree.getroot()
		for child in root.findall('crawl-job'):
			CrawlerBot.sites.append(child.get('site'))
			CrawlerBot.types.append(child.find('type').text)
			CrawlerBot.outs.append(child.find('out').text)

	# Reset for daemon mode.
	def reset(self):
		CrawlerBot.nofollow = False
		CrawlerBot.disallowed = []
		CrawlerBot.links = []
		CrawlerBot.titles = []
		CrawlerBot.descs = []
		CrawlerBot.keywords = [] 
		CrawlerBot.locs = []
		CrawlerBot.sitemaps = []

	# Run as daemon.
	def runAsDaemon(self, site, out, db, keyworded, sitemap, interval):
		CrawlerBot.daemon = True # I am a daemon.
		_print('Running {0} as daemon...'.format(CrawlerBot.robots_str))
		while True:
			if len(CrawlerBot.sites) > 0:
				self.doCrawls()
			else:			
				self.doCrawl(site, out, db, keyworded, sitemap)	
				self.reset()

			time.sleep(int(interval))

	# Do more than one crawl.
	def doCrawls(self):
		i = 0
		keyworded = False
		sitemap = False
		db = False
		for site in CrawlerBot.sites:
			type_is = CrawlerBot.types[i]
			if type_is == 'sitemap': 
				keyworded = False
				sitemap = True
			elif type_is == 'kw-sitemap':
				keyworded = True
				sitemap = True
			elif type_is == 'db':
				db = True
				sitemap = False

			self.doCrawl(site, CrawlerBot.outs[i], db, keyworded, sitemap)
			self.reset()
			i = i + 1

	# Do the crawl.
	def doCrawl(self, site, out, db, keyworded, sitemap):

		if os.path.isfile(CrawlerBot.cache): os.remove(CrawlerBot.cache)

		CrawlerBot.website = site
		CrawlerBot.current = site
		CrawlerBot.keyworded = keyworded
		CrawlerBot.links.append(site)

		_print('Crawling initiated at {0}.'.format(datetime.datetime.now()))
		
		request = urllib2.Request(site + '/robots.txt', None, CrawlerBot.headers)
		robots = urllib2.urlopen(request).read()
		parser = RobotsParser()
		parser.feed(robots.lstrip())

		request = urllib2.Request(site, None, CrawlerBot.headers)
		html = urllib2.urlopen(request).read()
		parser = MetaParser()
		parser.feed(html.lstrip())

		if keyworded:
			parser = TitleParser()
			parser.feed(html.lstrip())

		if CrawlerBot.nofollow == False:
			parser = LinkParser()
			parser.feed(html.lstrip())

			sorted_links = self.trimLinks(CrawlerBot.links, CrawlerBot.disallowed)
			for link in sorted_links:
				self.followLink(link)

		_print('Crawling terminated at {0}.'.format(datetime.datetime.now()))

		if db:
			if out == None: out = 'links.db'
			self.writeToDatabase(CrawlerBot.links, CrawlerBot.disallowed, out)
		elif sitemap:
			if keyworded == False:
				if out == None: out = 'sitemap.xml'
				self.writeXMLSitemap(CrawlerBot.links, CrawlerBot.disallowed, out)
			else:
				if out == None: out = 'kw-sitemap.xml'
				self.writeXMLKeywordedSitemap(
					CrawlerBot.locs, 
				 	CrawlerBot.disallowed,
				 	CrawlerBot.titles,
				 	CrawlerBot.descs,
				 	CrawlerBot.keywords,
				 	out)

	# Trim links of duplicates.
	def trimLinks(self, links, disallowed):
		sorted_links = set(links) 
		f_sorted_links = []
		for link in sorted_links:
			if link in disallowed:
				pass 
			else:
				f_sorted_links.append(link)
		return f_sorted_links

	# Trim titles of duplicates.
	def trimTitles(self, titles):
		sorted_titles = set(titles)
		f_sorted_titles = []
		for title in sorted_titles:
			f_sorted_titles.append(title)
		return f_sorted_titles

	# Trim descriptions of duplicates.
	def trimDescs(self, descs):
		sorted_descs = set(descs)
		f_sorted_descs = []
		for desc in sorted_descs:
			f_sorted_descs.append(desc)
		return f_sorted_descs

	# Trim keywords of duplicates.
	def trimKeywords(self, keywords):
		sorted_keywords = set(keywords)
		f_sorted_keywords = []
		for keyword in sorted_keywords:
			f_sorted_keywords.append(keyword)
		return f_sorted_keywords

	# Follow a link
	def followLink(self, link):
		CrawlerBot.current = link

		request = urllib2.Request(link, None, CrawlerBot.headers)
		html = urllib2.urlopen(request).read()
		parser = MetaParser()
		parser.feed(html.lstrip())

		if CrawlerBot.keyworded:
			parser = TitleParser()
			parser.feed(html.lstrip())

		if CrawlerBot.nofollow == False:
			parser = LinkParser()
			parser.feed(html.lstrip())
			_print('----------------------------------------------------------------')
			_print('Following link ---> {0}'.format(link))
			_print('----------------------------------------------------------------')

	# Write list of links to a SQLite database file.
	def writeToDatabase(self, links, disallowed, out):
		if os.path.isfile(out): os.remove(out)
		sorted_links = self.trimLinks(links, disallowed)
		conn = sqlite3.connect(out)
		c = conn.cursor()
		c.execute('''CREATE TABLE links (id INTEGER PRIMARY KEY AUTOINCREMENT, link VARCHAR(50))''') # Create table.
		for link in sorted_links:
			c.execute("INSERT INTO links (link) VALUES ('{0}')".format(link)) # Insert a row of data.
		# Save (commit) the changes.
		conn.commit()

		# Close the connection.
		conn.close()

	# Write list of links to an XML sitemap.
	def writeXMLSitemap(self, links, disallowed, out):
		sorted_links = self.trimLinks(links, disallowed)
		xmlns = 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
		timestamp = re.sub(' ', 'T', str(datetime.datetime.now()))
		timestamp = re.sub('\.\d{6}', '+00:00', timestamp)

		sitemap = xml.Document()
		urlset = sitemap.createElement('urlset')
		sitemap.appendChild(urlset)

		for link in sorted_links:
			url = sitemap.createElement('url')
			urlset.appendChild(url)
			loc = sitemap.createElement('loc')
			url.appendChild(loc)
			loc_is = sitemap.createTextNode(link)
			loc.appendChild(loc_is)
			lastmod = sitemap.createElement('lastmod')
			url.appendChild(lastmod)
			lastmod_is = sitemap.createTextNode(timestamp)
			lastmod.appendChild(lastmod_is)
			changefreq = sitemap.createElement('changefreq')
			url.appendChild(changefreq)
			changefreq_is = sitemap.createTextNode('daily')
			changefreq.appendChild(changefreq_is)
			priority = sitemap.createElement('priority')
			url.appendChild(priority)
			priority_is = sitemap.createTextNode('0.8')
			priority.appendChild(priority_is)

		if len(CrawlerBot.links) > 0:
			f = codecs.open(out, 'w', 'utf-8-sig')
			f.write(sitemap.toprettyxml(encoding='utf-8'))
			f.close()

			f = codecs.open(out, 'r', 'utf-8-sig')
			lines = f.readlines()
			f.close()
			lines[1] = re.sub('\<urlset\>', '<urlset ' + xmlns, lines[1])
		
			f = codecs.open(out, 'w', 'utf-8-sig')
			for line in lines:
				f.write(line)
			f.close()

	def writeXMLKeywordedSitemap(self, links, disallowed, titles, descs, keywords, out):
		xmlns = 'xmlns="https://github.io/CrawlerBot/keyworded-sitemap/1.0">'
		timestamp = re.sub(' ', 'T', str(datetime.datetime.now()))
		timestamp = re.sub('\.\d{6}', '+00:00', timestamp)

		sitemap = xml.Document()
		urlset = sitemap.createElement('urlset')
		sitemap.appendChild(urlset)

		sorted_links = []
		i = 0
		for link in links:
			if link in sorted_links:
				del titles[i]
				del descs[i]
				if len(keywords) > 0:
					del keywords[i]
			else:
				sorted_links.append(link)
			i = i + 1

		i = 0
		for link in sorted_links:
			url = sitemap.createElement('url')
			urlset.appendChild(url)
			title = sitemap.createElement('title')
			url.appendChild(title)
			title_is = sitemap.createTextNode(titles[i])
			title.appendChild(title_is)
			description = sitemap.createElement('description')
			url.appendChild(description)
			description_is = sitemap.createTextNode(descs[i])
			description.appendChild(description_is)
			if len(keywords) > 0:
				keywords = sitemap.createElement('keywords')
				url.appendChild(keywords)
				keywords_are = sitemap.createTextNode(keywords[i])
				keywords.appendChild(keywords_are)
			loc = sitemap.createElement('loc')
			url.appendChild(loc)
			loc_is = sitemap.createTextNode(link)
			loc.appendChild(loc_is)
			lastmod = sitemap.createElement('lastmod')
			url.appendChild(lastmod)
			lastmod_is = sitemap.createTextNode(timestamp)
			lastmod.appendChild(lastmod_is)
			changefreq = sitemap.createElement('changefreq')
			url.appendChild(changefreq)
			changefreq_is = sitemap.createTextNode('daily')
			changefreq.appendChild(changefreq_is)
			priority = sitemap.createElement('priority')
			url.appendChild(priority)
			priority_is = sitemap.createTextNode('0.8')
			priority.appendChild(priority_is)
			i = i + 1

		if len(CrawlerBot.locs) > 0:
			f = codecs.open(out, 'w', 'utf-8-sig')
			f.write(sitemap.toprettyxml(encoding='utf-8'))
			f.close()

			f = codecs.open(out, 'r', 'utf-8-sig')
			lines = f.readlines()
			lines[1] = re.sub('\<urlset\>', '<urlset ' + xmlns, lines[1])

			f = codecs.open(out, 'w', 'utf-8-sig')
			for line in lines:
				f.write(line)
			f.close()

# Parse robots.txt file for a site.
class RobotsParser():

	ua_match = '({0}|\*)'.format(CrawlerBot.robots_str)

	def feed(self, robots_file):
		i = 0
		robots_file = robots_file.split('\n')

		_print('Parsing /robots.txt from {0}...'.format(CrawlerBot.website))

		for line in robots_file:

			if line.startswith('User-agent:'): # Identify a user-agent directive and analyze it.
				ua_pair = line.split() # Split into 'User-agent:' and 'CrawlerBot|*'
				matched = re.match(RobotsParser.ua_match, ua_pair[1], re.IGNORECASE) # Applies to this crawler if it matches... 
				if matched != None:
					if matched.group(0): # ... CrawlerBot (any case) or * (wildcard): 
						_print('Identified a user-agent directive applicable to me (\'{0}\'):'.format(ua_pair[1]))
						break			
			i = i + 1

		for line in robots_file[i:]:

			if line.startswith('Sitemap:'):
				sitemap_pair = line.split()
				_print('I have found a sitemap: {0}'.format(sitemap_pair[1]))
				_print('Parsing sitemap...')
				request = urllib2.Request(sitemap_pair[1], None, CrawlerBot.headers)
				xml = urllib2.urlopen(request).read()
				parser = SitemapParser()
				parser.feed(xml.lstrip())

			elif line.startswith('Allow:'):
				allow_pair = line.split()
				_print('I am allowed ({0}-ed) to scan: {1}'.format(allow_pair[0][:-1], allow_pair[1]))

			elif line.startswith('Disallow:'):
					forbidden_pair = line.split()
					_print('I am forbidden ({0}-ed) from following: {1}'.format(forbidden_pair[0][:-1], forbidden_pair[1]))
					CrawlerBot.disallowed.append('{0}{1}'.format(CrawlerBot.website, forbidden_pair[1]))

					# When / is forbidden, CrawlerBot is cannot crawl specified site,so it will terminate.
					if forbidden_pair[1] == '/': 
						_print('I am forbidden from crawling {0}.\nI will comply.'.format(CrawlerBot.website))
						_print('Terminating...')
						sys.exit(1)

# Parse title for a site.
class TitleParser(HTMLParser):

	def handle_starttag(self, tag, attrs):
		tag = tag.lower() # Treat title tag as lowercase.

		if tag == 'title':
			CrawlerBot.is_title = True
		else:
			CrawlerBot.is_title = False

	def handle_data(self, data):
		if CrawlerBot.is_title:
			CrawlerBot.titles.append(data)

# Parse metadata for a site.
class MetaParser(HTMLParser):

	def handle_starttag(self, tag, attrs):
		tag = tag.lower() # Treat all tags as lowercase.

		if tag == 'meta':
			robots = False
			description = False
			keywords = False
			loc = False

			for attr in attrs:
				if attr[0].lower() == 'name':
					if attr[1].lower() == 'robots':
						robots = True
					elif attr[1].lower() == 'description':
						description = True
					elif attr[1].lower() == 'keywords':
						keywords = True

				elif attr[0].lower() == 'http-equiv':
					if attr[1].lower() == 'content-location':
						loc = True

				elif attr[0].lower() == 'content':
					if robots:
						if re.search('nofollow', attr[1], re.IGNORECASE):
							_print('I am not following links on this page as directed by META element...')
							CrawlerBot.nofollow = True

						if re.search('noindex', attr[1], re.IGNORECASE):
							_print('I will not index this page as directed by META element...')
							CrawlerBot.disallowed.append(CrawlerBot.current)

					elif description:
							CrawlerBot.descs.append(attr[1])

					elif keywords:
							CrawlerBot.keywords.append(attr[1])

					elif loc:
							CrawlerBot.locs.append('{0}/{1}'.format(CrawlerBot.website, attr[1]))
							CrawlerBot.cacheToDatabase('{0}/{1}'.format(CrawlerBot.website, attr[1]))

# Parse internal links for a site [a(href="http://site.xyz/link.ext")].
class LinkParser(HTMLParser):

	def handle_starttag(self, tag, attrs):
		tag = tag.lower() # Treat all tags as lowercase.

		# Store normal internal links.
		if tag == 'a' and attrs[0][0] == 'href':
			link = attrs[0][1]
			for attr in attrs:
				if attr[0] == 'rel' and attr[1] == 'nofollow':
					CrawlerBot.nofollow = True

			full = re.search('^https?://|#', link)	# Identify out-bound (external) links and ignore them.
			if full or CrawlerBot.nofollow:
				if full == None: link = '{0}/{1}'.format(CrawlerBot.website, link).lstrip()
				link = re.sub('\/index\.\w{3,4}', '', link)
				_print('Ignoring external, \'nofollow\' or # link: {0}'.format(link))

			else:
				link = '{0}/{1}'.format(CrawlerBot.website, link).lstrip()
				link = re.sub('\/index\.\w{3,4}', '', link)
				link = re.sub('/{2}', '/', link)
				link = re.sub('p:/', 'p://', link)
				_print('Processing link: {0}'.format(link))
			
				CrawlerBot.links.append(link)
				CrawlerBot.cacheToDatabase(link)

# Parse sitemap XML.
class SitemapParser():

	def feed(self, sitemap):
		dom = xml.parseString(sitemap)
		self.handleSitemap(dom)
		
	def handleSitemap(self, sitemap):
		sitemaps = sitemap.getElementsByTagName('sitemap')
		for sitemap in sitemaps:
			loc = sitemap.getElementsByTagName('loc')[0].childNodes[0].data
			if loc.endswith('.xml'):
				_print('Processing sitemap: {0}'.format(loc))
				CrawlerBot.sitemaps.append(loc)
			else:
				_print('Processing link: {0}'.format(loc))
				CrawlerBot.links.append(loc)

# Print to stdout or stderr as applicable.
def _print(message):
	if CrawlerBot.daemon and CrawlerBot.verbose:
		sys.stderr.write(message + '\n')

	elif CrawlerBot.verbose:
		print(message)

# Handle any command line arguments.
parser = argparse.ArgumentParser(description='CrawlerBot: A website crawler which can'
+ ' store scraped links in a SQLite database or generate an XML sitemap.')
parser.add_argument('-f', '--file', action='store', dest='file', metavar="FILE")
parser.add_argument('-s', '--site', action='store', dest='site', metavar="SITE")
parser.add_argument('-o', '--out', action='store', dest='out', metavar="OUT")
parser.add_argument('-d', '--db', action='store_true', dest='db')
parser.add_argument('-x', '--xml-sitemap', action='store_true', dest='sitemap')
parser.add_argument('-k', '--keyworded', action='store_true', dest='keyworded')
parser.add_argument('-l', '--verbose', action='store_true', dest='verbose')
parser.add_argument('-v', '--version', action='store_true', dest='version')
parser.add_argument('-i', '--info', action='store_true', dest='info')
parser.add_argument('-m', '--daemon', action='store_true', dest='daemon')
parser.add_argument('-p', '--interval', action='store', dest='interval', metavar="INTERVAL")
argv = parser.parse_args()

CrawlerBot(argv.file, argv.site, argv.out, argv.db, argv.sitemap, argv.keyworded, 
argv.verbose, argv.version, argv.info, argv.daemon, argv.interval)
