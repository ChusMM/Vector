# -*- coding: utf-8 -*-
import xml.dom.minidom
from xml.dom.minidom import Document

class XMLDocument:

	def __init__(self, listRetreived, setWords):
		# Create the minidom document
		self.doc = Document()
		self.listRetreived = listRetreived
		self.setWords = setWords
	
	def getXML(self):
		# <!DOCTYPE Resultado SYSTEM "Resultados.dtd">
		self.doc.appendChild(self.doc.createProcessingInstruction("xml-stylesheet", "type=\"text/xsl\" href=\"Resultados.xsl\""))
		
		Resultado = self.doc.createElement("Resultado")
		self.doc.appendChild(Resultado)

		Pregunta = self.doc.createElement("Pregunta")
		Resultado.appendChild(Pregunta)

		for word in self.setWords:
		
			Item = self.doc.createElement("Item")
			Pregunta.appendChild(Item)
			textItem = self.doc.createTextNode("%s" %(word))
			Item.appendChild(textItem)

		for element in self.listRetreived:
		
			Documento = self.doc.createElement("Documento")
			Documento.setAttribute("ID", "%s" %(element[0]))
			Resultado.appendChild(Documento)

			Titulo = self.doc.createElement("Titulo")
			Documento.appendChild(Titulo)
			textTitulo = self.doc.createTextNode("%s" %(element[1]))
			Titulo.appendChild(textTitulo)

			Relevancia = self.doc.createElement("Relevancia")
			Documento.appendChild(Relevancia)
			textRelevancia = self.doc.createTextNode('%s' %(element[3]))
			Relevancia.appendChild(textRelevancia)

			Texto = self.doc.createElement("Texto")
			Documento.appendChild(Texto)
			text = self.doc.createTextNode("%s" %(element[2]))
			Texto.appendChild(text)

		return self.doc.toprettyxml(indent="\t")
