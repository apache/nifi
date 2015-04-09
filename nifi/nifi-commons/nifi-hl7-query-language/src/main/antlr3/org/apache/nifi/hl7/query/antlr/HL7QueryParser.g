parser grammar HL7QueryParser;

options {
	output=AST;
	tokenVocab=HL7QueryLexer;
}

tokens {
	QUERY;
	DECLARATION;
}

@header {
	package org.apache.nifi.hl7.query.antlr;
	import org.apache.nifi.hl7.query.exception.HL7QueryParsingException;
}

@members {
  public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
  	final StringBuilder sb = new StringBuilder();
    if ( e.token == null ) {
    	sb.append("Unrecognized token ");
    } else {
    	sb.append("Unexpected token '").append(e.token.getText()).append("' ");
    }
    sb.append("at line ").append(e.line);
    if ( e.approximateLineInfo ) {
    	sb.append(" (approximately)");
    }
    sb.append(", column ").append(e.charPositionInLine);
    sb.append(". Query: ").append(e.input.toString());
    
    throw new HL7QueryParsingException(sb.toString());
  }

  public void recover(final RecognitionException e) {
  	final StringBuilder sb = new StringBuilder();
    if ( e.token == null ) {
    	sb.append("Unrecognized token ");
    } else {
    	sb.append("Unexpected token '").append(e.token.getText()).append("' ");
    }
    sb.append("at line ").append(e.line);
    if ( e.approximateLineInfo ) {
    	sb.append(" (approximately)");
    }
    sb.append(", column ").append(e.charPositionInLine);
    sb.append(". Query: ").append(e.input.toString());
    
    throw new HL7QueryParsingException(sb.toString());
  } 
}


declareClause : DECLARE^ declaration (COMMA! declaration)*;

requiredOrOptional : REQUIRED | OPTIONAL;
declaration : IDENTIFIER AS requiredOrOptional SEGMENT_NAME ->
	^(DECLARATION IDENTIFIER requiredOrOptional SEGMENT_NAME);


selectClause : SELECT^ selectableClause;
selectableClause : selectable (COMMA! selectable)*;
selectable : (MESSAGE | ref | field)^ (AS! IDENTIFIER^)?;


whereClause : WHERE^ conditions;

conditions : condition ((AND^ | OR^) condition)*;

condition : NOT^ condition | LPAREN! conditions RPAREN! | evaluation;

evaluation : expression
			 (
			 	unaryOperator^
			 	| (binaryOperator^ expression)
			 );

expression : (LPAREN! expr RPAREN!) | expr;
expr : ref | field | STRING_LITERAL | NUMBER;

unaryOperator : IS_NULL | NOT_NULL;
binaryOperator : EQUALS | NOT_EQUALS | LT | GT | LE | GE;

ref : (SEGMENT_NAME | IDENTIFIER);
field : ref DOT^ NUMBER 
	(DOT^ NUMBER (DOT^ NUMBER (DOT^ NUMBER)?)?)?;


query : declareClause? selectClause whereClause? EOF ->
	^(QUERY declareClause? selectClause whereClause?);
