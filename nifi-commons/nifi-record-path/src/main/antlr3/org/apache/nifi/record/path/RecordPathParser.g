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
parser grammar RecordPathParser;

options {
	output=AST;
	tokenVocab=RecordPathLexer;
}

tokens {
	PATH_EXPRESSION;
	PATH;
	FIELD_NAME;
	ROOT_REFERENCE;
	CHILD_REFERENCE;
	DESCENDANT_REFERENCE;
	PARENT_REFERENCE;
	STRING_LIST;
	ARRAY_INDEX;
	NUMBER_LIST;
	NUMBER_RANGE;
	MAP_KEY;
	ARRAY_INDEX;
	PREDICATE;
	OPERATOR;
	RELATIVE_PATH;
}

@header {
	package org.apache.nifi.record.path;
	import org.apache.nifi.record.path.exception.RecordPathException;
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

    throw new RecordPathException(sb.toString());
  }
}




// Literals
multipleStringLiterals : STRING_LITERAL (COMMA! STRING_LITERAL)*;

stringList : multipleStringLiterals ->
	^(STRING_LIST multipleStringLiterals);

rawOrLiteral : RAW_FIELD_NAME | STRING_LITERAL;




//
// Filtering
//
mapKey : stringList ->
	^(MAP_KEY stringList);

range : NUMBER RANGE NUMBER ->
	^(NUMBER_RANGE NUMBER NUMBER);

numberOrRange : NUMBER | range;

multipleIndices : numberOrRange (COMMA numberOrRange)* ->
	^(NUMBER_LIST numberOrRange numberOrRange*);

arrayIndex : multipleIndices ->
	^(ARRAY_INDEX multipleIndices);

indexOrKey : mapKey | arrayIndex | WILDCARD;

index : LBRACKET! indexOrKey RBRACKET!;



//
// Predicates
//
operator : LESS_THAN | LESS_THAN_EQUAL | GREATER_THAN | GREATER_THAN_EQUAL | EQUAL | NOT_EQUAL;

literal : NUMBER | STRING_LITERAL;

expression : path | literal;

operation : relativePath operator^ expression;

predicate : LBRACKET operation RBRACKET ->
	^(PREDICATE operation);




//
// References
//

fieldName : rawOrLiteral ->
	^(FIELD_NAME rawOrLiteral);

wildcardFieldName : fieldName | WILDCARD;

childReference : CHILD_SEPARATOR wildcardFieldName ->
	^(CHILD_REFERENCE wildcardFieldName);

descendantReference : DESCENDANT_SEPARATOR wildcardFieldName ->
	^(DESCENDANT_REFERENCE wildcardFieldName);

rootReference : CHILD_SEPARATOR ->
	^(CHILD_REFERENCE);

selfReference : CHILD_SEPARATOR! CURRENT_FIELD;

parentReference : CHILD_SEPARATOR RANGE ->
	^(PARENT_REFERENCE);

nonSelfFieldRef : childReference | descendantReference | selfReference | parentReference;

fieldRef : nonSelfFieldRef | CURRENT_FIELD;

subPath : fieldRef | index | predicate;



//
// Paths
//

pathSegment : fieldRef subPath* ->
	^(PATH fieldRef subPath*);

absolutePathSegment : nonSelfFieldRef subPath* ->
	^(PATH nonSelfFieldRef subPath*);

absolutePath : rootReference | absolutePathSegment;

relativePathSegment : nonSelfFieldRef subPath* ->
	^(RELATIVE_PATH nonSelfFieldRef subPath*);

initialParentReference : RANGE ->
	^(PARENT_REFERENCE);

currentOrParent : CURRENT_FIELD | initialParentReference;

relativePath : currentOrParent relativePathSegment? ->
	^(RELATIVE_PATH currentOrParent relativePathSegment?);

path : absolutePath | relativePath;

pathExpression : path EOF ->
	^(PATH_EXPRESSION path);
