# -*- coding: utf-8 -*-
# Indexador para la práctica de ARI, 239 documentos
# Jesús Manuel Muñoz Mazuecos, Ingeniería Informática.
import os
import re
import sys
import MySQLdb
from stat import *
from string import split
from string import lower

class indexer:

	def __init__(self):
		self.stopList = indexer.__fillStopList(self)
	
	def __getConn(self):
		try:
			conn = MySQLdb.connect(host = "localhost", 
							    	    port = 3307, 
								        user = "root", 
								        passwd = "9648", 
								        db = "ARI")
			return conn
			
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)
	
	def __closeConn(self, conn):
		conn.close()
				
	def __insertIntoPostingFile(self, conn, doc, word, frecuency):
		try:
			cursor = conn.cursor()
			cursor.execute('INSERT INTO posting_file (di, tj, ftij)' +
						   ' VALUES (%d, "%s", %d)' %(doc, word, frecuency))
			cursor.close()
			conn.commit()
		
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			conn.rollback();
			sys.exit(1)
		
	def __insertIntoDocuments(self, conn, doc, path):
		try:
			cursor = conn.cursor()
			cursor.execute('INSERT INTO documents (name, path)'+
						   ' VALUES ("%s", "%s")' %(doc, path))
			cursor.close()
			conn.commit()
		
		except MySQLdb.Error, e:
			print "Error %d: %s" %(e.args[0], e.args[1])
			conn.rollback();
			sys.exit(1)

	def __insertIntoDictionary(self, conn, word):
		try:						   
			cursor = conn.cursor()
			cursor.execute('SELECT tj FROM dictionary' +
						   ' WHERE dictionary.tj = "%s"' %(word))
			row = cursor.fetchone()
		
			if row == None:
				cursor.execute('INSERT INTO dictionary (tj, fdj)' +
							   ' VALUES ("%s", %d)' %(word, 1))
			else:
				cursor.execute('UPDATE dictionary' + 
							   ' SET dictionary.fdj = dictionary.fdj + 1' +
							   ' WHERE dictionary.tj = "%s"' %(word))		
			cursor.close()
			conn.commit()
		
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			conn.rollback();
			sys.exit(1)
			
	def __fillStopList(self):
		doc = open(os.getcwd() + '/stopList.txt', 'r')
		stopList = []
	
		for line in doc:
			stopList = stopList + split(line)
		return set(stopList)

	def __deleteContent(self, conn):
		try:
			cursor = conn.cursor()

			cursor.execute('delete from posting_file');
			cursor.execute('delete from dictionary');
			cursor.execute('delete from documents');
			print 'Contenido de las tablas borrado.'
		
			cursor.close()
			conn.commit()
		
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			conn.rollback();
			sys.exit(1)
		
	def __getIdLastDoc(self, conn):
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT COUNT(id) FROM documents')
			row = cursor.fetchone()
			
			cursor.close()
			return int(row[0])
			
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)
	
	def getStopList(self):
		return self.stopList

	def dropTables(self):
		try:
			conn = indexer.__getConn(self)
			cursor = conn.cursor()
			cursor.execute('drop table posting_file');
			cursor.execute('drop table dictionary');
			cursor.execute('drop table documents');
			print 'Tablas borradas.'
		
			cursor.close()
			conn.commit()
		
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			conn.rollback();
			sys.exit(1)
			
	def getDocuments(self):
		try:
			docs = []
			conn = indexer.__getConn(self)
			cursor = conn.cursor()
			cursor.execute('SELECT path FROM documents')
			resultSet = cursor.fetchall()
	
			for row in resultSet:
				docs.append(str(row[0]))
				
			cursor.close()
			indexer.__closeConn(self, conn)
			return docs
			
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)
			
	def fillFrecuencies(self):
		path = os.getcwd() + '/Docs'
		
		conn = indexer.__getConn(self)
		indexer.__deleteContent(self, conn)
		indexer.__closeConn(self, conn)

		for f in os.listdir(path):		
			indexer.registerNewDocument(self, f, path)
				
	def registerNewDocument(self, f, path):
		dic = {}
		wordList = []
		conn = indexer.__getConn(self)
		
		pathname = os.path.join(path, f)
		mode = os.stat(pathname).st_mode
		
		if S_ISREG(mode):
			# It's a file
			print pathname
			
			indexer.__insertIntoDocuments(self, conn, f, pathname)
			idDoc = indexer.__getIdLastDoc(self, conn)
			doc = open(pathname, 'r')
			
			for line in doc:
				s = lower(line)
				wordsLine = re.split('\W+|_+', s)
				wordList = wordList + wordsLine
			
			wordSet = set(wordList)
			wordSet.remove('')
				
			wordSet = wordSet - self.stopList
		
			for word in wordSet:
				n = wordList.count(word)
				indexer.__insertIntoDictionary(self, conn, word)
				indexer.__insertIntoPostingFile(self, conn, idDoc, word, n)
		else:
			print 'Skipping %s' % pathname
			
		indexer.__closeConn(self, conn)
