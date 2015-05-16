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
import codecs
import argparse
import datetime
import urllib2
import sqlite3
from HTMLParser import HTMLParser
import xml.dom.minidom as xml

# This class handles the crawling itself.
class CrawlerBot:

	disallowed = []
	links = []

	verbose = False # Not verbose by default.
	robots_str = 'CrawlerBot' # User agent string as used in a robots.txt user agent directive.
	ua_str = 'Mozilla/5.0 (compatible; CrawlerBot/1.0; +https://github.com/stpettersens/crawlerbot)' 
	headers = { 'User-Agent': ua_str }
	# UA string as used in HTTP requests.
	website = 'http://www.something.com' # Dummy value for website to crawl; will be overriden on invocation.

	cache = 'cache.db'

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

	def __init__(self, site, out, db, sitemap, verbose, version, info):
		if len(sys.argv) == 1 or info: 
			print(__doc__) # Display program information.
		elif version:
			print(CrawlerBot.ua_str) # Print user-agent string, which contains version (1.0).
		else:
			if verbose == True: CrawlerBot.verbose = True # Set to be verbose.
			self.doCrawl(site, out, db, sitemap)

	# Do the crawl.
	def doCrawl(self, site, out, db, sitemap):

		if os.path.isfile(CrawlerBot.cache): os.remove(CrawlerBot.cache)

		CrawlerBot.website = site
		
		request = urllib2.Request(site + '/robots.txt', None, CrawlerBot.headers)
		robots = urllib2.urlopen(request).read()
		parser = RobotsParser()
		parser.feed(robots.lstrip())

		request = urllib2.Request(site, None, CrawlerBot.headers)
		html = urllib2.urlopen(request).read()
		parser = MetaLinkParser()
		parser.feed(html.lstrip())

		sorted_links = self.trimLinks(CrawlerBot.links, CrawlerBot.disallowed)
		for link in sorted_links:
			self.followLink(link)

		if db:
			if out == None: out = 'links.db'
			self.writeToDatabase(CrawlerBot.links, CrawlerBot.disallowed, out)
		elif sitemap:
			if out == None: out = 'sitemap.xml'
			self.writeXMLSitemap(CrawlerBot.links, CrawlerBot.disallowed, out)

	# Trim links of duplicates
	def trimLinks(self, links, disallowed):
		sorted_links = set(links) 
		f_sorted_links = []
		for link in sorted_links:
			if link in disallowed:
				pass
			else:
				f_sorted_links.append(link)
		return f_sorted_links

	# Follow a link
	def followLink(self, link):
		if CrawlerBot.verbose:
			print('----------------------------------------------------------------')
			print('Following link ---> {0}'.format(link))
			print('----------------------------------------------------------------')
		request = urllib2.Request(link, None, CrawlerBot.headers)
		html = urllib2.urlopen(request).read()
		parser = MetaLinkParser()
		parser.feed(html.lstrip())

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

		if len(links) > 0:
			sitemap = xml.Document()
			urlset = sitemap.createElement('urlset')
			sitemap.appendChild(urlset)
		elif CrawlerBot.verbose:
			print('No links to generate sitemap for, terminating...')
			sys.exit(0)

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

# Parse robots.txt file for a site.
class RobotsParser():

	disallowed = []
	ua_match = '({0}|\*)'.format(CrawlerBot.robots_str)

	def feed(self, robots_file):
		i = 0
		x = 1
		robots_file = robots_file.split('\n')
		if CrawlerBot.verbose:
			print('Parsing /robots.txt from {0}...'.format(CrawlerBot.website))

		for line in robots_file:

			if line.startswith('User-agent:'): # Identify a user-agent directive and analyze it.
				ua_pair = line.split() # Split into 'User-agent:' and 'CrawlerBot|*'
				matched = re.search(RobotsParser.ua_match, ua_pair[1], re.IGNORECASE) # Applies to this crawler if it matches... 
				if matched != None:
					if matched.group(0): # ... CrawlerBot (any case) or * (wildcard).
						if CrawlerBot.verbose: 
							print('Identified a user-agent directive applicable to me (\'{0}\'):'.format(ua_pair[1]))
						break			
			i = i + 1

		for line in robots_file[i:]:

			if line.startswith('Sitemap:'):
				sitemap_pair = line.split()
				if CrawlerBot.verbose:
					print('I have found a sitemap: {0}'.format(sitemap_pair[1]))
					print('Parsing sitemap...')
				pass # TODO...
				#request = urllib2.Request(sitemap_pair[1], None, CrawlerBot.headers)
				#xml = urllib2.urlopen(request).read()
				#parser = SitemapParser()
				#parser.feed(xml.lstrip())
				#sys.exit(0)

			elif line.startswith('Allow:'):
				allow_pair = line.split()
				if CrawlerBot.verbose:
					print('I am allowed ({0}-ed) to scan: {1}'.format(allow_pair[0][:-1], allow_pair[1])) 

			elif line.startswith('Disallow:'):
					forbidden_pair = line.split()
					if CrawlerBot.verbose:
						print('I am forbidden ({0}-ed) from following: {1}'.format(forbidden_pair[0][:-1], forbidden_pair[1]))

					CrawlerBot.disallowed.append('{0}{1}'.format(CrawlerBot.website, forbidden_pair[1]))

					# When / is forbidden, CrawlerBot is cannot crawl specified site,so it will terminate.
					if forbidden_pair[1] == '/': 
						if CrawlerBot.verbose:
							print('I am forbidden from crawling {0}.\nI will comply.'.format(CrawlerBot.website))
							print('Terminating...')
						sys.exit(-1)

# Parse internal links for a site [a(href="http://site.xyz/link.ext")].
class MetaLinkParser(HTMLParser):

	def handle_starttag(self, tag, attrs):

		if tag == 'a':
			print(attrs)

		# Do not store nofollow links.
		#if tag == 'a' and attrs[1][0] == 'rel' or tag == 'a' and attrs[2][0] == 'rel':
			#if attrs[1][0] == 'nofollow' or attrs[2][0] == 'nofollow':
				#if(CrawlerBot.verbose):
					#print('Skipping \'nofollow\' link: {0}'.format(attr[0][1]))

		# Store normal internal links.
		elif tag == 'a' and attrs[0][0] == 'href':
			print(attrs)
			link = attrs[0][1]
			full = re.search('^https?://|#', link)	# Identify out-bound (external) links and ignore them.
			if full:
				pass
			else:
				link = '{0}/{1}'.format(CrawlerBot.website, link).lstrip()
				link = re.sub('/{2}', '/', link)
				link = re.sub('p:/', 'p://', link)
				if CrawlerBot.verbose:
					print('Processing link: {0}'.format(link))
			
				CrawlerBot.links.append(link)
				CrawlerBot.cacheToDatabase(link)

# Parse sitemap XML.
class SitemapParser():

	def feed(self, sitemap):
		dom = xml.parseString(sitemap)
		self.handleSitemap(dom)

	def getText(self, nodelist):
		rc = []
		for node in nodelist:
			if node.nodeType == node.TEXT_NODE:
				rc.append(node.data)
		return ''.join(rc)
		
	def handleSitemap(self, sitemap):
		sitemaps = sitemap.getElementsByTagName('sitemap')
		for sitemap in sitemaps:
			loc = sitemap.getElementsByTagName('loc')
			self.handleLocText(loc)

	def handleLocText(self, loc):
		loc_is = '<loc>{0}</loc>'.format(self.getText(loc.childNodes))


# Handle any command line arguments.
parser = argparse.ArgumentParser(description='CrawlerBot: A website crawler which can'
+ ' store scraped links in a SQLite database or generate an XML sitemap.')
parser.add_argument('-s', '--site', action='store', dest='site', metavar="SITE")
parser.add_argument('-o', '--out', action='store', dest='out', metavar="OUT")
parser.add_argument('-d', '--db', action='store_true', dest='db')
parser.add_argument('-x', '--xml-sitemap', action='store_true', dest='sitemap')
parser.add_argument('-l', '--verbose', action='store_true', dest='verbose')
parser.add_argument('-v', '--version', action='store_true', dest='version')
parser.add_argument('-i', '--info', action='store_true', dest='info')
argv = parser.parse_args()

CrawlerBot(argv.site, argv.out, argv.db, argv.sitemap, argv.verbose, argv.version, argv.info)
