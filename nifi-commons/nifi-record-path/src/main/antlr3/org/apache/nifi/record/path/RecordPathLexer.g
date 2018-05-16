/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
lexer grammar RecordPathLexer;

@header {
	package org.apache.nifi.record.path;
	import org.apache.nifi.record.path.exception.RecordPathException;
}

@rulecatch {
  catch(final Exception e) {
    throw new RecordPathException(e);
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

    throw new RecordPathException(sb.toString());
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

    throw new RecordPathException(sb.toString());
  }
}


// PUNCTUATION & SPECIAL CHARACTERS
CHILD_SEPARATOR : '/';
DESCENDANT_SEPARATOR : '//';
LBRACKET : '[';
RBRACKET : ']';
LPAREN : '(';
RPAREN : ')';
NUMBER : '-'? ('0'..'9')+;
QUOTE : '\'';
COMMA : ',';
RANGE : '..';
CURRENT_FIELD : '.';

WILDCARD : '*';



// Operators
LESS_THAN : '<';
LESS_THAN_EQUAL : '<=';
GREATER_THAN : '>';
GREATER_THAN_EQUAL : '>=';
EQUAL : '=';
NOT_EQUAL : '!=';


WHITESPACE : SPACE+ { skip(); };
fragment SPACE : ' ' | '\t' | '\n' | '\r' | '\u000C';


// filter functions
CONTAINS : 'contains';
CONTAINS_REGEX : 'containsRegex';
ENDS_WITH : 'endsWith';
STARTS_WITH : 'startsWith';
IS_BLANK : 'isBlank';
IS_EMPTY : 'isEmpty';
MATCHES_REGEX : 'matchesRegex';
NOT : 'not';


IDENTIFIER : (
	~('/' | '[' | ']' | '*' | '"' | '\'' | ',' | '\t' | '\r' | '\n' | '0'..'9' | ' ' | '.' | '-' | '=' | '?' | '<' | '>' | '(' | ')' )
	~('/' | '[' | ']' | '*' | '"' | '\'' | ',' | '\t' | '\r' | '\n' | '=' | '?' | '<' | '>' | ' ' | '(' | ')' )*
);

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
