# -*- coding: utf-8 -*-
# Clase buscador.
# Jesús Manuel Muñoz Mazuecos, Ingeniería Informática.
import re
import sys
import math
import time
import MySQLdb
from copy import copy
from string import lower
from indexer import indexer
from operator import itemgetter
from XMLDocument import XMLDocument

class buscador:

	def __init__(self):
		print "Instancia creada"
		
	def __getConn(self):
		try:
			conn = MySQLdb.connect(host = 'localhost', 
								   port = 3307, 
								   user = 'root', 
								   passwd = '9648', 
								   db = 'ARI')
			return conn
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __getD(self, conn):
		try:
			cursor = conn.cursor()	
	
			cursor.execute('SELECT COUNT(id) FROM documents')
			row = cursor.fetchone()
	
			cursor.close()		
			return int(row[0])
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __getDocs(self, conn):
		listDocs = []
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT id FROM documents')
			resultSet = cursor.fetchall()
	
			for row in resultSet:
				listDocs.append(int(row[0]))
		
			cursor.close()
			return listDocs
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __buildEmptyVector(self, conn):
		vector = {}
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT tj FROM dictionary')
			resultSet = cursor.fetchall()
		
			for row in resultSet:
				vector[str(row[0])] = 0.0
		
			cursor.close()
			return vector
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)		
	
	def __getFdjs(self, conn):
		fdj = {}
		try:
			cursor = conn.cursor()
	
			cursor.execute('SELECT tj, fdj FROM dictionary')
		
			resultSet = cursor.fetchall()
		
			for row in resultSet:
				fdj[str(row[0])] = int(row[1])
	
			cursor.close()
			return fdj
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)
			
	def __getWords(self, conn, doc):
		wordList = []
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT tj FROM posting_file' + 
						   ' WHERE posting_file.di = %d' %(doc))
		
			resultSet = cursor.fetchall()
			for row in resultSet:
				wordList.append(row[0])
				
			cursor.close()
			return set(wordList)
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)
		
	def __frecuencies(self, conn, doc):
		ftij = {}
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT tj, ftij FROM posting_file' + 
						   ' WHERE posting_file.di = %d' %(doc))
		
			resultSet = cursor.fetchall()
			for row in resultSet:
				ftij[str(row[0])] = int(row[1])
			
			cursor.close()
			return ftij
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __nameDoc(self, conn, idDoc):
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT name FROM documents where id = %d' %(idDoc))
	
			row = cursor.fetchone()
			return str(row[0])
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __pathDoc(self, conn, idDoc):
		try:
			cursor = conn.cursor()
			cursor.execute('SELECT path FROM documents WHERE id = %d' %(idDoc))
	
			row = cursor.fetchone()
			return row[0]
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def __getWeight(self, ftij, d, fdj):
		if fdj == 0:
			return 0
		else:
			return float(float(ftij) * math.log(float(d) / float(fdj), 10))
	
	def __parseQuestion(self, question):
		weightDic = {}
		try:
			s = lower(question)
			l = re.split(', ', s)
		
			for item in l:
				lw = re.split('\W+', item)
				if len(lw) == 1:
					lw.append('1')
				
				weightDic[lw[0]] = float(lw[1])
				
			return weightDic
		except:
			print 'Formato de entrada incorrecto'

	def __buildQuestion(self, conn, question, d, emptyVector):

		empty = ['']
		questionVector = copy(emptyVector)
	
		index = indexer()
		stopList = index.getStopList()
	
		weightList = buscador.__parseQuestion(self, question)
	
		wordSet = set(weightList.keys())
	
		wordSet = wordSet - stopList
		wordSet = wordSet - set(empty)
	
		dicFdj = buscador.__getFdjs(self, conn)
	
		for word in wordSet:
			questionVector[word] = weightList[word]
	
		return wordSet, questionVector, dicFdj
	
	def __buildVectors(self, conn, emptyVector, d, dicFdj, wordSet):
		vectors = {}
		listDocs = buscador.__getDocs(self, conn)
	
		for doc in listDocs:
			dicFrecs = buscador.__frecuencies(self, conn, doc)
			
			wordsList = dicFrecs.keys()
			docSet = set(dicFrecs.keys())
			
			if len(docSet & wordSet) > 0:
				vector = copy(emptyVector)
		
				for key in dicFrecs.keys():
					weight = buscador.__getWeight(self, 
												  dicFrecs[key], 
												  d, 
												  dicFdj[key])
					vector[key] = weight		
				vectors[doc] = vector
				
		return vectors
				
	def __module(self, v):
		sumatory = float(0.0)
		for word in v.keys():
			sumatory = sumatory + (v[word] * v[word])
		
		return math.sqrt(sumatory)

	def __product(self, u, v):
		result = float(0.0)
		for key in u.keys():
			result += v[key] * u[key]
	
		return result

	def __similarity(self, vectors, question):
		dicAlphas = {}
	
		for key in vectors.keys():
			modV = buscador.__module(self, vectors[key])  
			modQ = buscador.__module(self, question)			
			p = buscador.__product(self, vectors[key], question)

			result = float(p / (modV * modQ))
			dicAlphas[key] = result * 100.0 #math.arcos(result)
		
		return dicAlphas
		
	def __getSortedList(self, conn, dicAlphas):
		listRetreived = []
	
		for key in dicAlphas.keys():
			var = key, buscador.__nameDoc(self, conn, key), buscador.__pathDoc(self, conn, key), dicAlphas[key]
			
			listRetreived.append(var)
		
		listRetreived = sorted(listRetreived, key = itemgetter(3), reverse = True)
		return listRetreived

	def getIdDoc(self, path):
		try:
			conn = buscador.__getConn(self)
			cursor = conn.cursor()
			cursor.execute('SELECT documents.id FROM documents' + 
						   ' WHERE documents.path = "%s"' %(path))
			row = cursor.fetchone()
	
			cursor.close()
			conn.close()
			return int(row[0])
		
		except MySQLdb.Error, e:
			print 'Error %d: %s' % (e.args[0], e.args[1])
			sys.exit(1)

	def buscar(self, question):

		Init = time.time()
	
		conn = buscador.__getConn(self)
		d = buscador.__getD(self, conn)
	
		emptyVector = buscador.__buildEmptyVector(self, conn)
		wordSet, questionVector, dicFdj = buscador.__buildQuestion(self, 
																  conn, 
																  question, 
																  d, 
																  emptyVector)
		
		vectors = buscador.__buildVectors(self, conn, emptyVector, d, dicFdj, wordSet)
	
		dicAlphas = buscador.__similarity(self, vectors, questionVector)
		listRetreived = buscador.__getSortedList(self, conn, dicAlphas)
		
		print '------> Pregunta: %s' %(question)
	
		XML = XMLDocument(listRetreived, wordSet)
	
		f = open('Resultados.xml', 'w')
		f.write(XML.getXML())
		f.close()
	
		print 'Busqueda finalizada'
		conn.close()
	
		userTime = time.time() - Init
		print 'Tiempo de ejecución: %.3f segundos' % (userTime)
		return userTime
	
	def similares(self, iden):
		Init = time.time()
				
		conn = buscador.__getConn(self)
		d = buscador.__getD(self, conn)
		dicFdj = buscador.__getFdjs(self, conn)
		
		emptyVector = buscador.__buildEmptyVector(self, conn)
		
		wordSet = buscador.__getWords(self, conn, iden)
		vectors = buscador.__buildVectors(self, conn, emptyVector, d, dicFdj, wordSet)
		
		questionVector = copy(vectors[iden])
		
		dicAlphas = buscador.__similarity(self, vectors, questionVector)
		listRetreived = buscador.__getSortedList(self, conn, dicAlphas)
		
		XML = XMLDocument(listRetreived, buscador.__nameDoc(self, conn, iden))
	
		f = open('Resultados.xml', 'w')
		f.write(XML.getXML())
		f.close()
	
		print 'Busqueda finalizada'
		conn.close()
		
		userTime = time.time() - Init
		print 'Tiempo de ejecución: %.3f segundos' % (userTime)
		return userTime
