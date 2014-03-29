<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
	<xsl:template match="Resultado">
		<html>
			<head>
				<title>Resultados de la busqueda</title>
			</head>
			<body>
				<h1>
					<p align="center">Resultados de la Búsqueda</p>
				</h1>
				<xsl:apply-templates select="Pregunta"/>
				<xsl:apply-templates select="Documento"/>
			</body>
		</html>
	</xsl:template>
	<xsl:template match="Pregunta">
		<p>
			<b>Pregunta: </b>
			<xsl:apply-templates select="Item"/>
		</p>
	</xsl:template>
	<xsl:template match="Item">
		<i>
			<xsl:apply-templates/>&#032;</i>
	</xsl:template>
	<xsl:template match="Documento">
		<p>
			<b>
				<xsl:value-of select="@ID"/>
			</b>
			&#032;-&#032;
			<b>
				<i>
					<xsl:apply-templates select="Titulo"/>
				</i>
			</b>&#032;
			<xsl:apply-templates select="Relevancia"/>
			<a>
				<xsl:attribute name="href">
					<xsl:apply-templates select="Texto"/>
				</xsl:attribute>
				Documento
			</a>
		</p>
	</xsl:template>
</xsl:stylesheet>
