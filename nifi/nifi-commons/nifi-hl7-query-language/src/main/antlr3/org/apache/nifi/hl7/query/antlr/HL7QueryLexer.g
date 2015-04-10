lexer grammar HL7QueryLexer;

@header {
	package org.apache.nifi.hl7.query.antlr;
	import org.apache.nifi.hl7.query.exception.HL7QueryParsingException;
}

@rulecatch {
  catch(final Exception e) {
    throw new HL7QueryParsingException(e);
  }
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

  public void recover(RecognitionException e) {
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


// PUNCTUATION & SPECIAL CHARACTERS
WHITESPACE : (' '|'\t'|'\n'|'\r')+ { $channel = HIDDEN; };
COMMENT : '#' ( ~('\n') )* '\n' { $channel = HIDDEN; };

LPAREN	: '(';
RPAREN	: ')';
LBRACE  : '{';
RBRACE  : '}';
COLON	: ':';
COMMA	: ',';
DOT		: '.';
SEMICOLON : ';';



// OPERATORS
EQUALS		: '=';
NOT_EQUALS	: '!=';
GT			: '>';
GE			: '>=';
LT			: '<';
LE			: '<=';
REGEX		: 'MATCHES REGEX';
LIKE		: 'LIKE';
IS_NULL		: 'IS NULL';
NOT_NULL	: 'NOT NULL';


// KEYWORDS
AND			: 'AND';
OR			: 'OR';
NOT			: 'NOT';

TRUE	: 'true';
FALSE	: 'false';

SELECT		: 'select' | 'SELECT';
DECLARE		: 'declare' | 'DECLARE';
OPTIONAL	: 'optional' | 'OPTIONAL';
REQUIRED	: 'required' | 'REQUIRED';
AS			: 'as' | 'AS';
WHERE		: 'where' | 'WHERE';

MESSAGE 	: 'MESSAGE' | 'message';
SEGMENT 	: 'SEGMENT' | 'segment';


SEGMENT_NAME : LETTER ALPHA_NUMERIC ALPHA_NUMERIC;


NUMBER	: ('0'..'9')+;
fragment LETTER : 'A'..'Z';
fragment ALPHA_NUMERIC : 'A'..'Z' | '0'..'9';


// STRINGS
STRING_LITERAL
@init{StringBuilder lBuf = new StringBuilder();}
	:
		(
			'"'
				(
					escaped=ESC {lBuf.append(getText());} |
				  	normal = ~( '"' | '\\' | '\n' | '\r' | '\t' ) { lBuf.appendCodePoint(normal);} 
				)*
			'"'
		)
		{
			setText(lBuf.toString());
		}
		|
		(
			'\''
				(
					escaped=ESC {lBuf.append(getText());} |
				  	normal = ~( '\'' | '\\' | '\n' | '\r' | '\t' ) { lBuf.appendCodePoint(normal);} 
				)*
			'\''
		)
		{
			setText(lBuf.toString());
		}
		;


fragment
ESC
	:	'\\'
		(
				'"'		{ setText("\""); }
			|	'\''	{ setText("\'"); }
			|	'r'		{ setText("\r"); }
			|	'n'		{ setText("\n"); }
			|	't'		{ setText("\t"); }
			|	'\\'	{ setText("\\\\"); }
			|	nextChar = ~('"' | '\'' | 'r' | 'n' | 't' | '\\')		
				{
					StringBuilder lBuf = new StringBuilder(); lBuf.append("\\\\").appendCodePoint(nextChar); setText(lBuf.toString());
				}
		)
	;

IDENTIFIER : (
				  ~('$' | '{' | '}' | '(' | ')' | '[' | ']' | ',' | ':' | ';' | '/' | '*' | '\'' | ' ' | '\t' | '\r' | '\n' | '0'..'9' | '.')
				  ~('$' | '{' | '}' | '(' | ')' | '[' | ']' | ',' | ':' | ';' | '/' | '*' | '\'' | ' ' | '\t' | '\r' | '\n' | '.')*
				 );
