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
package org.apache.nifi.attribute.expression.language;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.registry.VariableRegistry;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestQuery {

    @Test
    public void testCompilation() {
        assertInvalid("${attr:uuid()}");
        assertInvalid("${attr:indexOf(length())}");
        assertValid("${UUID()}");
        assertInvalid("${UUID():nextInt()}");
        assertValid("${nextInt()}");
        assertValid("${now():format('yyyy/MM/dd')}");
        assertInvalid("${attr:times(3)}");
        assertValid("${attr:toNumber():multiply(3)}");
        assertValid("${hostname()}");
        assertValid("${literal(3)}");
        assertValid("${random()}");
        assertValid("${getStateValue('the_count')}");
        // left here because it's convenient for looking at the output
        //System.out.println(Query.compile("").evaluate(null));
    }

    @Test
    public void testPrepareWithEscapeChar() {
        final Map<String, String> variables = Collections.singletonMap("foo", "bar");

        assertEquals("bar${foo}$bar", Query.prepare("${foo}$${foo}$$${foo}").evaluateExpressions(variables, null));

        final PreparedQuery onlyEscapedQuery = Query.prepare("$${foo}");
        final String onlyEscapedEvaluated = onlyEscapedQuery.evaluateExpressions(variables, null);
        assertEquals("${foo}", onlyEscapedEvaluated);

        final PreparedQuery mixedQuery = Query.prepare("${foo}$${foo}");
        final String mixedEvaluated = mixedQuery.evaluateExpressions(variables, null);
        assertEquals("bar${foo}", mixedEvaluated);
    }

    private void assertValid(final String query) {
        try {
            Query.compile(query);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Expected query to be valid, but it failed to compile due to " + e);
        }
    }

    private void assertInvalid(final String query) {
        try {
            Query.compile(query);
            Assert.fail("Expected query to be invalid, but it did compile");
        } catch (final Exception e) {
        }
    }

    @Test
    public void testIsValidExpression() {
        Query.validateExpression("${abc:substring(${xyz:length()})}", false);
        Query.isValidExpression("${now():format('yyyy-MM-dd')}");

        try {
            Query.validateExpression("$${attr}", false);
            Assert.fail("invalid query validated");
        } catch (final AttributeExpressionLanguageParsingException e) {
        }

        Query.validateExpression("$${attr}", true);

        Query.validateExpression("${filename:startsWith('T8MTXBC')\n"
                + ":or( ${filename:startsWith('C4QXABC')} )\n"
                + ":or( ${filename:startsWith('U6CXEBC')} )"
                + ":or( ${filename:startsWith('KYM3ABC')} )}", false);
    }

    @Test
    public void testCompileEmbedded() {
        final String expression = "${x:equals( ${y} )}";
        final Query query = Query.compile(expression);
        final Tree tree = query.getTree();
        System.out.println(printTree(tree));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "x");
        attributes.put("y", "x");
        final String result = Query.evaluateExpressions(expression, attributes, null);
        assertEquals("true", result);

        Query.validateExpression(expression, false);
    }

    private String printTree(final Tree tree) {
        final StringBuilder sb = new StringBuilder();
        printTree(tree, 0, sb);

        return sb.toString();
    }

    private void printTree(final Tree tree, final int spaces, final StringBuilder sb) {
        for (int i = 0; i < spaces; i++) {
            sb.append(" ");
        }

        if (tree.getText().trim().isEmpty()) {
            sb.append(tree.toString()).append("\n");
        } else {
            sb.append(tree.getText()).append("\n");
        }

        for (int i = 0; i < tree.getChildCount(); i++) {
            printTree(tree.getChild(i), spaces + 2, sb);
        }
    }

    @Test
    public void testEscape() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "hello");
        attributes.put("$xyz", "good-bye");
        attributes.put("${abc}", "good-bye");

        assertEquals("$$xyz", evaluateQueryForEscape("$$$$${abc}", attributes));
        assertEquals("xyz", evaluateQueryForEscape("${abc}", attributes));
        assertEquals("${abc}", evaluateQueryForEscape("$${abc}", attributes));
        assertEquals("$xyz", evaluateQueryForEscape("$$${abc}", attributes));
        assertEquals("$${abc}", evaluateQueryForEscape("$$$${abc}", attributes));

        assertEquals( "Unescaped $$${5 because no closing brace", evaluateQueryForEscape("Unescaped $$${5 because no closing brace", attributes));
        assertEquals( "Unescaped $ because no closing brace", evaluateQueryForEscape("Unescaped $$${'5'} because no closing brace", attributes));

        assertEquals("I owe you $5", evaluateQueryForEscape("I owe you $5", attributes));
        assertEquals("You owe me $$5 too", evaluateQueryForEscape("You owe me $$5 too", attributes));
        assertEquals("Unescaped $$${5 because no closing brace", evaluateQueryForEscape("Unescaped $$${5 because no closing brace", attributes));
        assertEquals("xyz owes me $5", evaluateQueryForEscape("${abc} owes me $5", attributes));
        assertEquals("xyz owes me ${5", evaluateQueryForEscape("${abc} owes me ${5", attributes));
        assertEquals("xyz owes me ", evaluateQueryForEscape("${abc} owes me ${'5'}", attributes));
        assertEquals("xyz owes me $", evaluateQueryForEscape("${abc} owes me $$${'5'}", attributes));

        assertEquals("SNAP$$$$$$$$$", evaluateQueryForEscape("${literal('SNAP$$$$$$$$$')}", attributes));
        assertEquals("SNAP$$", evaluateQueryForEscape("${literal('SNAP$$')}", attributes));
        assertEquals("hello", evaluateQueryForEscape("${${abc}}", attributes));
        assertEquals("good-bye", evaluateQueryForEscape("${'$$${abc}'}", attributes));
        assertEquals("good-bye", evaluateQueryForEscape("${'$xyz'}", attributes));
    }

    @Test
    public void testWithBackSlashes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "C:\\test\\1.txt");
        attributes.put("y", "y\ny");

        final String query = "${x:substringAfterLast( '/' ):substringAfterLast( '\\\\' )}";
        verifyEquals(query, attributes, "1.txt");
        attributes.put("x", "C:/test/1.txt");
        verifyEquals(query, attributes, "1.txt");

        verifyEquals("${y:equals('y\\ny')}", attributes, Boolean.TRUE);
    }

    @Test
    public void testWithTicksOutside() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "My Value");
        assertEquals(1, Query.extractExpressionRanges("\"${attr}").size());
        assertEquals(1, Query.extractExpressionRanges("'${attr}").size());
        assertEquals(1, Query.extractExpressionRanges("'${attr}'").size());
        assertEquals(1, Query.extractExpressionRanges("${attr}").size());

        assertEquals("'My Value'", Query.evaluateExpressions("'${attr}'", attributes, null));
        assertEquals("'My Value", Query.evaluateExpressions("'${attr}", attributes, null));
    }

    @Test
    public void testDateToNumber() {
        final Query query = Query.compile("${dateTime:toDate('yyyy/MM/dd HH:mm:ss.SSS', 'America/New_York'):toNumber()}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dateTime", "2013/11/18 10:22:27.678");

        final QueryResult<?> result = query.evaluate(attributes);
        assertEquals(ResultType.WHOLE_NUMBER, result.getResultType());
        assertEquals(1384788147678L, result.getValue());
    }

    @Test
    public void testAddOneDayToDate() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dateTime", "2013/11/18 10:22:27.678");

        verifyEquals("${dateTime:toDate('yyyy/MM/dd HH:mm:ss.SSS'):toNumber():plus(86400000):toDate():format('yyyy/MM/dd HH:mm:ss.SSS')}", attributes, "2013/11/19 10:22:27.678");
        verifyEquals("${dateTime:toDate('yyyy/MM/dd HH:mm:ss.SSS'):plus(86400000):format('yyyy/MM/dd HH:mm:ss.SSS')}", attributes, "2013/11/19 10:22:27.678");
    }

    @Test
    @Ignore("Requires specific locale")
    public void implicitDateConversion() {
        final Date date = new Date();
        final Query query = Query.compile("${dateTime:format('yyyy/MM/dd HH:mm:ss.SSS')}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dateTime", date.toString());

        // the date.toString() above will end up truncating the milliseconds. So remove millis from the Date before
        // formatting it
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS", Locale.US);
        final long millis = date.getTime() % 1000L;
        final Date roundedToNearestSecond = new Date(date.getTime() - millis);
        final String formatted = sdf.format(roundedToNearestSecond);

        final QueryResult<?> result = query.evaluate(attributes);
        assertEquals(ResultType.STRING, result.getResultType());
        assertEquals(formatted, result.getValue());
    }

    @Test
    public void testEmbeddedExpressionsAndQuotes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "abc");
        attributes.put("a", "abc");

        verifyEquals("${x:equals(${a})}", attributes, true);

        Query.validateExpression("${x:equals('${a}')}", false);
        assertEquals("true", Query.evaluateExpressions("${x:equals('${a}')}", attributes, null));

        Query.validateExpression("${x:equals(\"${a}\")}", false);
        assertEquals("true", Query.evaluateExpressions("${x:equals(\"${a}\")}", attributes, null));
    }

    @Test
    public void testJsonPath() throws IOException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("json", getResourceAsString("/json/address-book.json"));
        verifyEquals("${json:jsonPath('$.firstName')}", attributes, "John");
        verifyEquals("${json:jsonPath('$.address.postalCode')}", attributes, "10021-3100");
        verifyEquals("${json:jsonPath(\"$.phoneNumbers[?(@.type=='home')].number\")}", attributes, "212 555-1234");
        verifyEquals("${json:jsonPath('$.phoneNumbers')}", attributes,
                "[{\"type\":\"home\",\"number\":\"212 555-1234\"},{\"type\":\"office\",\"number\":\"646 555-4567\"}]");
        verifyEquals("${json:jsonPath('$.missing-path')}", attributes, "");
        try {
            verifyEquals("${json:jsonPath('$..')}", attributes, "");
            Assert.fail("Did not detect bad JSON path expression");
        } catch (final AttributeExpressionLanguageException e) {
        }
        try {
            verifyEquals("${missing:jsonPath('$.firstName')}", attributes, "");
            Assert.fail("Did not detect empty JSON document");
        } catch (AttributeExpressionLanguageException e) {
        }
        attributes.put("invalid", "[}");
        try {
            verifyEquals("${invlaid:jsonPath('$.firstName')}", attributes, "John");
            Assert.fail("Did not detect invalid JSON document");
        } catch (AttributeExpressionLanguageException e) {
        }
    }

    @Test
    public void testEmbeddedExpressionsAndQuotesWithProperties() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "abc");
        attributes.put("a", "abc");

        verifyEquals("${x:equals(${a})}", attributes, true);

        Query.validateExpression("${x:equals('${a}')}", false);
        assertEquals("true", Query.evaluateExpressions("${x:equals('${a}')}", attributes, null));

        Query.validateExpression("${x:equals(\"${a}\")}", false);
        assertEquals("true", Query.evaluateExpressions("${x:equals(\"${a}\")}", attributes, null));
    }

    @Test
    public void testJoin() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a.a", "a");
        attributes.put("a.b", "b");
        attributes.put("a.c", "c");
        verifyEquals("${allAttributes( 'a.a', 'a.b', 'a.c' ):join(', ')}", attributes, "a, b, c");
        verifyEquals("${x:join(', ')}", attributes, "");
        verifyEquals("${a.a:join(', ')}", attributes, "a");
        verifyEquals("${allAttributes( 'x', 'y' ):join(',')}", attributes, ",");
    }

    @Test(expected = AttributeExpressionLanguageException.class)
    public void testCannotCombineWithNonReducingFunction() {
        Query.compile("${allAttributes( 'a.1' ):plus(1)}");
    }

    @Test
    public void testIsEmpty() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "a");
        attributes.put("b", "");
        attributes.put("c", "        \n");

        verifyEquals("${a:isEmpty()}", attributes, false);
        verifyEquals("${b:isEmpty()}", attributes, true);
        verifyEquals("${c:isEmpty()}", attributes, true);
        verifyEquals("${d:isEmpty()}", attributes, true);
    }

    @Test
    public void testReplaceEmpty() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "a");
        attributes.put("b", "");
        attributes.put("c", "        \n");

        verifyEquals("${a:replaceEmpty('c')}", attributes, "a");
        verifyEquals("${b:replaceEmpty('c')}", attributes, "c");
        verifyEquals("${c:replaceEmpty('c')}", attributes, "c");
        verifyEquals("${d:replaceEmpty('c')}", attributes, "c");
    }

    @Test
    public void testCount() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "a");
        attributes.put("b", "abc");
        attributes.put("c", "        \n");
        attributes.put("n1", "111");
        attributes.put("n2", "222");
        attributes.put("n3", "333333");

        verifyEquals("${allMatchingAttributes( '.*' ):count()}", attributes, 6L);
        verifyEquals("${allMatchingAttributes( '.*' ):length():gt(2):count()}", attributes, 5L);
        verifyEquals("${allMatchingAttributes( 'n.*' ):plus(1):count()}", attributes, 3L);
    }

    @Test
    public void testCurlyBracesInQuotes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "My Valuee");

        assertEquals("Val", evaluateQueryForEscape("${attr:replaceAll('My (Val)ue{1,2}', '$1')}", attributes));
        assertEquals("Val", evaluateQueryForEscape("${attr:replaceAll(\"My (Val)ue{1,2}\", '$1')}", attributes));
    }

    @SuppressWarnings("unchecked")
    private String evaluateQueryForEscape(final String queryString, final Map<String, String> attributes) {
        final FlowFile mockFlowFile = Mockito.mock(FlowFile.class);
        Mockito.when(mockFlowFile.getAttributes()).thenReturn(attributes);
        Mockito.when(mockFlowFile.getId()).thenReturn(1L);
        Mockito.when(mockFlowFile.getEntryDate()).thenReturn(System.currentTimeMillis());
        Mockito.when(mockFlowFile.getSize()).thenReturn(1L);
        Mockito.when(mockFlowFile.getLineageStartDate()).thenReturn(System.currentTimeMillis());

        final ValueLookup lookup = new ValueLookup(VariableRegistry.EMPTY_REGISTRY, mockFlowFile);
        return Query.evaluateExpressions(queryString, lookup);
    }

    @Test
    public void testGetAttributeValue() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "My Value");
        verifyEquals("${attr}", attributes, "My Value");
    }

    @Test
    public void testGetAttributeValueEmbedded() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "XX ");
        attributes.put("XX", "My Value");
        verifyEquals("${${attr:trim()}}", attributes, "My Value");
    }

    @Test
    public void testSimpleSubstring() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "My Value");
        verifyEquals("${attr:substring(2, 5)}", attributes, " Va");
    }

    @Test
    public void testCallToFunctionWithSubjectResultOfAnotherFunctionCall() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "   My Value   ");
        verifyEquals("${attr:trim():substring(2, 5)}", attributes, " Va");
    }

    @Test
    public void testProblematic1() {
        // There was a bug that prevented this expression from compiling. This test just verifies that it now compiles.
        final String queryString = "${xx:append( \"120101\" ):toDate( 'yyMMddHHmmss' ):format( \"yy-MM-dd’T’HH:mm:ss\") }";
        Query.compile(queryString);
    }

    @Test
    public void testEquals() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", " XX    ");
        verifyEquals("${attr:trim():equals('XX')}", attributes, true);
    }

    @Test
    public void testDeeplyEmbedded() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "false");
        attributes.put("abc", "a");
        attributes.put("a", "a");

        verifyEquals("${x:or( ${${abc}:length():equals(1)} )}", attributes, true);
    }

    @Test
    public void testExtractExpressionRanges() {
        assertEquals(29, Query.extractExpressionRanges("${hello:equals( $${goodbye} )}").get(0).getEnd());

        List<Range> ranges = Query.extractExpressionRanges("hello");
        assertTrue(ranges.isEmpty());

        ranges = Query.extractExpressionRanges("${hello");
        assertTrue(ranges.isEmpty());

        ranges = Query.extractExpressionRanges("hello}");
        assertTrue(ranges.isEmpty());

        ranges = Query.extractExpressionRanges("$${hello");
        assertTrue(ranges.isEmpty());

        ranges = Query.extractExpressionRanges("$he{ll}o");
        assertTrue(ranges.isEmpty());

        ranges = Query.extractExpressionRanges("${hello}");
        assertEquals(1, ranges.size());
        Range range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(7, range.getEnd());

        ranges = Query.extractExpressionRanges("${hello:equals( ${goodbye} )}");
        assertEquals(1, ranges.size());
        range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(28, range.getEnd());

        ranges = Query.extractExpressionRanges("${hello:equals( $${goodbye} )}");
        assertEquals(1, ranges.size());
        range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(29, range.getEnd());

        ranges = Query.extractExpressionRanges("${hello:equals( $${goodbye} )} or just hi, ${bob:or(${jerry})}");
        assertEquals(2, ranges.size());
        range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(29, range.getEnd());

        range = ranges.get(1);
        assertEquals(43, range.getStart());
        assertEquals(61, range.getEnd());

        ranges = Query.extractExpressionRanges("${hello:equals( ${goodbye} )} or just hi, ${bob}, are you ${bob.age:toNumber()} yet? $$$${bob}");
        assertEquals(3, ranges.size());
        range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(28, range.getEnd());

        range = ranges.get(1);
        assertEquals(42, range.getStart());
        assertEquals(47, range.getEnd());

        range = ranges.get(2);
        assertEquals(58, range.getStart());
        assertEquals(78, range.getEnd());

        ranges = Query.extractExpressionRanges("${x:matches( '.{4}' )}");
        assertEquals(1, ranges.size());
        range = ranges.get(0);
        assertEquals(0, range.getStart());
        assertEquals(21, range.getEnd());
    }

    @Test
    public void testExtractExpressionTypes() {
        final List<ResultType> types = Query.extractResultTypes("${hello:equals( ${goodbye} )} or just hi, ${bob}, are you ${bob.age:toNumber()} yet? $$$${bob}");
        assertEquals(3, types.size());
        assertEquals(ResultType.BOOLEAN, types.get(0));
        assertEquals(ResultType.STRING, types.get(1));
        assertEquals(ResultType.WHOLE_NUMBER, types.get(2));
    }

    @Test
    public void testEqualsEmbedded() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("x", "hello");
        attributes.put("y", "good-bye");

        verifyEquals("${x:equals( ${y} )}", attributes, false);

        attributes.put("y", "hello");
        verifyEquals("${x:equals( ${y} )}", attributes, true);

        attributes.put("x", "4");
        attributes.put("y", "3");
        attributes.put("z", "1");
        attributes.put("h", "100");
        verifyEquals("${x:toNumber():lt( ${y:toNumber():plus( ${h:toNumber()} )} )}", attributes, true);
        verifyEquals("${h:toNumber():ge( ${y:toNumber():plus( ${z:toNumber()} )} )}", attributes, true);
        verifyEquals("${x:toNumber():equals( ${y:toNumber():plus( ${z:toNumber()} )} )}", attributes, true);

        attributes.put("x", "88");
        verifyEquals("${x:toNumber():gt( ${y:toNumber():plus( ${z:toNumber()} )} )}", attributes, true);

        attributes.put("y", "88");
        assertEquals("true", Query.evaluateExpressions("${x:equals( '${y}' )}", attributes, null));
    }

    @Test
    public void testComplicatedEmbeddedExpressions() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fox", "quick, brown");
        attributes.put("dog", "lazy");

        verifyEquals("${fox:substring( ${ 'dog' :substring(2):length()}, 5 ):equals( 'ick' )}", attributes, true);
        verifyEquals("${fox:substring( ${ 'dog' :substring(2):length()}, 5 ):equals( 'ick' )}", attributes, true);
    }

    @Test
    public void testQuotingQuotes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xx", "say 'hi'");

        String query = "${xx:replaceAll( \"'.*'\", '\\\"hello\\\"' )}";
        verifyEquals(query, attributes, "say \"hello\"");

        query = "${xx:replace( \"'\", '\"')}";
        verifyEquals(query, attributes, "say \"hi\"");

        query = "${xx:replace( '\\'', '\"')}";
        System.out.println(query);
        verifyEquals(query, attributes, "say \"hi\"");
    }

    @Test
    public void testDoubleQuotesWithinSingleQuotes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xx", "say 'hi'");

        final String query = "${xx:replace( \"'hi'\", '\\\"hello\\\"' )}";
        System.out.println(query);
        verifyEquals(query, attributes, "say \"hello\"");
    }

    @Test
    public void testEscapeQuotes() {
        final long timestamp = 1403620278642L;
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("date", String.valueOf(timestamp));

        final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

        final String query = "startDateTime=\"${date:toNumber():toDate():format(\"" + format + "\")}\"";
        final String result = Query.evaluateExpressions(query, attributes, null);

        final String expectedTime = new SimpleDateFormat(format, Locale.US).format(timestamp);
        assertEquals("startDateTime=\"" + expectedTime + "\"", result);

        final List<Range> ranges = Query.extractExpressionRanges(query);
        assertEquals(1, ranges.size());
    }

    @Test
    public void testDateConversion() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("date", "1403620278642");

        verifyEquals("${date:format('yyyy')}", attributes, "2014");
        verifyEquals("${date:toDate():format('yyyy')}", attributes, "2014");
        verifyEquals("${date:toNumber():format('yyyy')}", attributes, "2014");
        verifyEquals("${date:toNumber():toDate():format('yyyy')}", attributes, "2014");
        verifyEquals("${date:toDate():toNumber():format('yyyy')}", attributes, "2014");
        verifyEquals("${date:toDate():toNumber():toDate():toNumber():toDate():toNumber():format('yyyy')}", attributes, "2014");
    }

    @Test
    public void testSingleLetterAttribute() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("A", "0123456789");

        verifyEquals("${A}", attributes, "0123456789");
        verifyEquals("${'A'}", attributes, "0123456789");
    }

    @Test
    public void testImplicitConversions() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("A", "0123456789");
        attributes.put("b", "true");
        attributes.put("c", "false");
        attributes.put("d", "Quick Brown Fox");
        attributes.put("F", "-48");
        attributes.put("n", "2014/04/04 00:00:00");

        final Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2014);
        cal.set(Calendar.MONTH, 3);
        cal.set(Calendar.DAY_OF_MONTH, 4);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 45);

        final String dateString = cal.getTime().toString();
        attributes.put("z", dateString);

        verifyEquals("${A:plus(4)}", attributes, 123456793L);
        verifyEquals("${A:plus( ${F} )}", attributes, 123456741L);

        verifyEquals("${F:lt( ${A} )}", attributes, true);
        verifyEquals("${A:substring(2,3):plus(21):substring(1,2):plus(0)}", attributes, 3L);
        verifyEquals("${n:format( 'yyyy' )}", attributes, "2014");
        verifyEquals("${z:format( 'yyyy' )}", attributes, "2014");

        attributes.put("n", "2014/04/04 00:00:00.045");
        verifyEquals("${n:format( 'yyyy' ):append(','):append( ${n:format( 'SSS' )} )}", attributes, "2014,045");
    }

    @Test
    public void testNewLinesAndTabsInQuery() {
        final String query = "${ abc:equals('abc'):or( \n\t${xx:isNull()}\n) }";
        assertEquals(ResultType.BOOLEAN, Query.getResultType(query));
        Query.validateExpression(query, false);
        assertEquals("true", Query.evaluateExpressions(query, Collections.emptyMap()));
    }

    @Test
    public void testAttributeReferencesWithWhiteSpace() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("a b c,d", "abc");

        final String query = "${ 'a b c,d':equals('abc') }";
        verifyEquals(query, attrs, true);
    }

    @Test
    public void testComments() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        final String expression
                = "# hello, world\n"
                + "${# ref attr\n"
                + "\t"
                + "abc"
                + "\t"
                + "#end ref attr\n"
                + "}";

        Query query = Query.compile(expression);
        QueryResult<?> result = query.evaluate(attributes);
        assertEquals(ResultType.STRING, result.getResultType());
        assertEquals("xyz", result.getValue());

        query = Query.compile("${abc:append('# hello') #good-bye \n}");
        result = query.evaluate(attributes);
        assertEquals(ResultType.STRING, result.getResultType());
        assertEquals("xyz# hello", result.getValue());
    }

    @Test
    public void testAppendPrepend() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "XX");
        attributes.put("YXXX", "bingo");

        verifyEquals("${${attr:append('X'):prepend('Y')}}", attributes, "bingo");
    }

    @Test
    public void testIsNull() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${attr:isNull()}", attributes, true);
    }

    @Test
    public void testNotNull() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "");

        verifyEquals("${attr:notNull()}", attributes, true);
    }

    @Test
    public void testIsNullOrLengthEquals0() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "");
        attributes.put("xyz", "xyz");
        attributes.put("xx", "  ");

        verifyEquals("${abc:isNull():or( ${abc:length():equals(0)} )}", attributes, true);
        verifyEquals("${xyz:isNull():or( ${xyz:length():equals(0)} )}", attributes, false);
        verifyEquals("${none:isNull():or( ${none:length():equals(0)} )}", attributes, true);
        verifyEquals("${xx:isNull():or( ${xx:trim():length():equals(0)} )}", attributes, true);
    }

    @Test
    public void testReplaceNull() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${attr:replaceNull('hello')}", attributes, "hello");
    }

    @Test
    public void testReplace() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "hello");
        verifyEquals("${attr:replace('hell', 'yell')}", attributes, "yello");
    }

    @Test
    public void testReplaceAll() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "hello");
        attributes.put("xyz", "00-00TEST.2014_01_01_000000_value");

        verifyEquals("${xyz:replaceAll(\"^([^.]+)\\.([0-9]{4})_([0-9]{2})_([0-9]{2}).*$\", \"$3\")}", attributes, "01");
        verifyEquals("${attr:replaceAll('l+', 'r')}", attributes, "hero");

        attributes.clear();
        attributes.put("filename1", "abc.gz");
        attributes.put("filename2", "abc.g");
        attributes.put("filename3", "abc.gz.gz");
        attributes.put("filename4", "abc.gz.g");
        attributes.put("abc", "hello world");

        verifyEquals("${filename3:replaceAll('\\\\\\.gz$', '')}", attributes, "abc.gz.gz");
        verifyEquals("${filename3:replaceAll('\\\\\\\\.gz$', '')}", attributes, "abc.gz.gz");
        verifyEquals("${filename1:replaceAll('\\.gz$', '')}", attributes, "abc");
        verifyEquals("${filename2:replaceAll('\\.gz$', '')}", attributes, "abc.g");
        verifyEquals("${filename4:replaceAll('\\\\.gz$', '')}", attributes, "abc.gz.g");

        verifyEquals("${abc:replaceAll( 'lo wor(ld)', '$0')}", attributes, "hello world");
        verifyEquals("${abc:replaceAll( 'he(llo) world', '$1')}", attributes, "llo");
        verifyEquals("${abc:replaceAll( 'xx', '$0')}", attributes, "hello world");
        verifyEquals("${abc:replaceAll( '(xx)', '$1')}", attributes, "hello world");
        verifyEquals("${abc:replaceAll( 'lo wor(ld)', '$1')}", attributes, "helld");

    }

    @Test
    public void testReplaceAllWithOddNumberOfBackslashPairs() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "C:\\temp\\.txt");

        verifyEquals("${filename:replace('\\\\', '/')}", attributes, "C:/temp/.txt");
        verifyEquals("${filename:replaceAll('\\\\\\\\', '/')}", attributes, "C:/temp/.txt");
        verifyEquals("${filename:replaceAll('\\\\\\.txt$', '')}", attributes, "C:\\temp");
    }

    @Test
    public void testReplaceAllWithMatchingGroup() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "hello");

        verifyEquals("${attr:replaceAll('.*?(l+).*', '$1')}", attributes, "ll");
    }

    @Test
    public void testMathWholeNumberOperations() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("one", "1");
        attributes.put("two", "2");
        attributes.put("three", "3");
        attributes.put("four", "4");
        attributes.put("five", "5");
        attributes.put("hundred", "100");

        verifyEquals("${hundred:toNumber():multiply(${two}):divide(${three}):plus(${one}):mod(${five})}", attributes, 2L);
    }

    @Test
    public void testMathDecimalOperations() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("first", "1.5");
        attributes.put("second", "12.3");
        attributes.put("third", "3");
        attributes.put("fourth", "4.201");
        attributes.put("fifth", "5.1");
        attributes.put("hundred", "100");

        // The expected resulted is calculated instead of a set number due to the inaccuracy of double arithmetic
        verifyEquals("${hundred:toNumber():multiply(${second}):divide(${third}):plus(${first}):mod(${fifth})}", attributes, (((100 * 12.3) / 3) + 1.5) %5.1);
    }

    @Test
    public void testMathResultInterpretation() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ten", "10.1");
        attributes.put("two", "2.2");

        // The expected resulted is calculated instead of a set number due to the inaccuracy of double arithmetic
        verifyEquals("${ten:divide(${two:plus(3)}):toNumber()}", attributes, (Double.valueOf(10.1 / (2.2 + 3)).longValue()));

        // The expected resulted is calculated instead of a set number due to the inaccuracy of double arithmetic
        verifyEquals("${ten:divide(${two:plus(3)}):toDecimal()}", attributes, (10.1 / (2.2 + 3)));

        // The expected resulted is calculated instead of a set number due to the inaccuracy of double arithmetic
        verifyEquals("${ten:divide(${two:plus(3.1)}):toDecimal()}", attributes, (10.1 / (2.2 + 3.1)));

        verifyEquals("${ten:divide(${two:plus(3)}):toDate():format(\"SSS\")}", attributes, "001");
    }

    @Test
    public void testMathFunction() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("one", "1");
        attributes.put("two", "2");
        attributes.put("oneDecimal", "1.5");
        attributes.put("twoDecimal", "2.3");
        attributes.put("negative", "-64");
        attributes.put("negativeDecimal", "-64.1");

        // Test that errors relating to not finding methods are properly handled
        try {
            verifyEquals("${math('rand'):toNumber()}", attributes, 0L);
            fail();
        } catch (AttributeExpressionLanguageException expected) {
            assertEquals("Cannot evaluate 'math' function because no subjectless method was found with the name:'rand'", expected.getMessage());
        }
        try {
            verifyEquals("${negativeDecimal:math('absolute')}", attributes, 0L);
            fail();
        } catch (AttributeExpressionLanguageException expected) {
            assertEquals("Cannot evaluate 'math' function because no method was found matching the passed parameters: name:'absolute', one argument of type: 'double'", expected.getMessage());
        }
        try {
            verifyEquals("${oneDecimal:math('power', ${two:toDecimal()})}", attributes, 0L);
            fail();
        } catch (AttributeExpressionLanguageException expected) {
            assertEquals("Cannot evaluate 'math' function because no method was found matching the passed parameters: name:'power', " +
                    "first argument type: 'double', second argument type:  'double'", expected.getMessage());
        }
        try {
            verifyEquals("${oneDecimal:math('power', ${two})}", attributes, 0L);
            fail();
        } catch (AttributeExpressionLanguageException expected) {
            assertEquals("Cannot evaluate 'math' function because no method was found matching the passed parameters: name:'power', " +
                    "first argument type: 'double', second argument type:  'long'", expected.getMessage());
        }

        // Can only verify that it runs. ToNumber() will verify that it produced a number greater than or equal to 0.0 and less than 1.0
        verifyEquals("${math('random'):toNumber()}", attributes, 0L);

        verifyEquals("${negative:math('abs')}", attributes, 64L);
        verifyEquals("${negativeDecimal:math('abs')}", attributes, 64.1D);

        verifyEquals("${negative:math('max', ${two})}", attributes, 2L);
        verifyEquals("${negativeDecimal:math('max', ${twoDecimal})}", attributes, 2.3D);

        verifyEquals("${oneDecimal:math('pow', ${two:toDecimal()})}", attributes, Math.pow(1.5,2));
        verifyEquals("${oneDecimal:math('scalb', ${two})}", attributes, Math.scalb(1.5,2));

        verifyEquals("${negative:math('abs'):toDecimal():math('cbrt'):math('max', ${two:toDecimal():math('pow',${oneDecimal}):mod(${two})})}", attributes,
                Math.max(Math.cbrt(Math.abs(-64)), Math.pow(2,1.5)%2));
    }

    @Test
    public void testMathLiteralOperations() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ten", "10.1");
        attributes.put("two", "2.2");

        // The expected resulted is calculated instead of a set number due to the inaccuracy of double arithmetic
        verifyEquals("${literal(5):toNumber():multiply(${two:plus(1)})}", attributes, 5*3.2);
        verifyEquals("${literal(5.5E-1):toDecimal():plus(${literal(.5E1)}):multiply(${two:plus(1)})}", attributes, (0.55+5)*3.2);
    }

    @Test
    public void testIndexOf() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "https://abc.go");
        verifyEquals("${attr:indexOf('/')}", attributes, 6L);
    }

    @Test
    public void testDate() {
        final Calendar now = Calendar.getInstance();
        final int year = now.get(Calendar.YEAR);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("entryDate", String.valueOf(now.getTimeInMillis()));

        verifyEquals("${entryDate:toNumber():toDate():format('yyyy')}", attributes, String.valueOf(year));

        // test for not existing attribute (NIFI-1962)
        assertEquals("", Query.evaluateExpressions("${notExistingAtt:toDate()}", attributes, null));

        attributes.clear();
        attributes.put("month", "3");
        attributes.put("day", "4");
        attributes.put("year", "2013");
        assertEquals("63", Query.evaluateExpressions("${year:append('/'):append(${month}):append('/'):append(${day}):toDate('yyyy/MM/dd'):format('D')}", attributes, null));
        assertEquals("63", Query.evaluateExpressions("${year:append('/'):append('${month}'):append('/'):append('${day}'):toDate('yyyy/MM/dd'):format('D')}", attributes, null));

        verifyEquals("${year:append('/'):append(${month}):append('/'):append(${day}):toDate('yyyy/MM/dd'):format('D')}", attributes, "63");
    }

    @Test
    public void testAnyAttribute() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "zzz");
        attributes.put("xyz", "abc");

        verifyEquals("${anyAttribute('abc', 'xyz', 'missingAttr'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyAttribute('abc', 'xyz'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyAttribute('xyz', 'abc'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyAttribute('zz'):substring(1,2):equals('b')}", attributes, false);
        verifyEquals("${anyAttribute('abc', 'zz'):isNull()}", attributes, true);
    }

    @Test
    public void testAnyMatchingAttribute() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "zzz");
        attributes.put("xyz", "abc");
        attributes.put("123.cba", "hello");

        verifyEquals("${anyMatchingAttribute('.{2}x', '.{2}z'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyMatchingAttribute('.*'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyMatchingAttribute('x{44}'):substring(1,2):equals('b')}", attributes, false);
        verifyEquals("${anyMatchingAttribute('abc'):substring(1,2):equals('b')}", attributes, false);
        verifyEquals("${anyMatchingAttribute('xyz'):substring(1,2):equals('b')}", attributes, true);
        verifyEquals("${anyMatchingAttribute('xyz'):notNull()}", attributes, true);
        verifyEquals("${anyMatchingAttribute('xyz'):isNull()}", attributes, false);
        verifyEquals("${anyMatchingAttribute('xxxxxxxxx'):notNull()}", attributes, false);
        verifyEquals("${anyMatchingAttribute('123\\.c.*'):matches('hello')}", attributes, true);
        verifyEquals("${anyMatchingAttribute('123\\.c.*|a.c'):matches('zzz')}", attributes, true);
    }

    @Test
    public void testAnyDelineatedValue() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "a,b,c");
        attributes.put("xyz", "abc");

        final String query = "${anyDelineatedValue('${abc}', ','):equals('b')}";
        assertEquals(ResultType.BOOLEAN, Query.getResultType(query));

        assertEquals("true", Query.evaluateExpressions(query, attributes, null));
        assertEquals("true", Query.evaluateExpressions("${anyDelineatedValue('${abc}', ','):equals('a')}", attributes, null));
        assertEquals("true", Query.evaluateExpressions("${anyDelineatedValue('${abc}', ','):equals('c')}", attributes, null));
        assertEquals("false", Query.evaluateExpressions("${anyDelineatedValue('${abc}', ','):equals('d')}", attributes, null));

        verifyEquals("${anyDelineatedValue(${abc}, ','):equals('b')}", attributes, true);
        verifyEquals("${anyDelineatedValue(${abc}, ','):equals('a')}", attributes, true);
        verifyEquals("${anyDelineatedValue(${abc}, ','):equals('c')}", attributes, true);
        verifyEquals("${anyDelineatedValue(${abc}, ','):equals('d')}", attributes, false);
    }

    @Test
    public void testNestedAnyDelineatedValueOr() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "a,b,c");
        attributes.put("xyz", "x");

        // Assert each part separately.
        assertEquals("true", Query.evaluateExpressions("${anyDelineatedValue('${abc}', ','):equals('c')}",
                attributes, null));
        assertEquals("false", Query.evaluateExpressions("${anyDelineatedValue('${xyz}', ','):equals('z')}",
                attributes, null));

        // Combine them with 'or'.
        assertEquals("true", Query.evaluateExpressions(
                "${anyDelineatedValue('${abc}', ','):equals('c'):or(${anyDelineatedValue('${xyz}', ','):equals('z')})}",
                attributes, null));
    }

    @Test
    public void testNestedAnyDelineatedValueAnd() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "2,0,1,3");
        attributes.put("xyz", "x,y,z");

        // Assert each part separately.
        assertEquals("true", Query.evaluateExpressions("${anyDelineatedValue('${abc}', ','):gt('2')}",
                attributes, null));
        assertEquals("true", Query.evaluateExpressions("${anyDelineatedValue('${xyz}', ','):equals('z')}",
                attributes, null));

        // Combine them with 'and'.
        assertEquals("true", Query.evaluateExpressions(
                "${anyDelineatedValue('${abc}', ','):gt('2'):and(${anyDelineatedValue('${xyz}', ','):equals('z')})}",
                attributes, null));
    }

    @Test
    public void testAllDelineatedValues() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "a,b,c");
        attributes.put("xyz", "abc");

        final String query = "${allDelineatedValues('${abc}', ','):matches('[abc]')}";

        assertEquals(ResultType.BOOLEAN, Query.getResultType(query));
        assertEquals("true", Query.evaluateExpressions(query, attributes, null));
        assertEquals("true", Query.evaluateExpressions(query, attributes, null));
        assertEquals("false", Query.evaluateExpressions("${allDelineatedValues('${abc}', ','):matches('[abd]')}", attributes, null));
        assertEquals("false", Query.evaluateExpressions("${allDelineatedValues('${abc}', ','):equals('a'):not()}", attributes, null));

        verifyEquals("${allDelineatedValues(${abc}, ','):matches('[abc]')}", attributes, true);
        verifyEquals("${allDelineatedValues(${abc}, ','):matches('[abd]')}", attributes, false);
        verifyEquals("${allDelineatedValues(${abc}, ','):equals('a'):not()}", attributes, false);
    }

    @Test
    public void testAllDelineatedValuesCount() {
        final Map<String, String> attributes = new HashMap<>();

        final String query = "${allDelineatedValues('${test}', '/'):count()}";

        attributes.put("test", "/my/path");
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType(query));
        assertEquals("3", Query.evaluateExpressions(query, attributes, null));
        assertEquals("", Query.evaluateExpressions("${test:getDelimitedField(1, '/')}", attributes, null));
        assertEquals("my", Query.evaluateExpressions("${test:getDelimitedField(2, '/')}", attributes, null));
        assertEquals("path", Query.evaluateExpressions("${test:getDelimitedField(3, '/')}", attributes, null));

        attributes.put("test", "this/is/my/path");
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType(query));
        assertEquals("4", Query.evaluateExpressions(query, attributes, null));
        assertEquals("this", Query.evaluateExpressions("${test:getDelimitedField(1, '/')}", attributes, null));
        assertEquals("is", Query.evaluateExpressions("${test:getDelimitedField(2, '/')}", attributes, null));
        assertEquals("my", Query.evaluateExpressions("${test:getDelimitedField(3, '/')}", attributes, null));
        assertEquals("path", Query.evaluateExpressions("${test:getDelimitedField(4, '/')}", attributes, null));

        attributes.put("test", "/");
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType(query));
        assertEquals("0", Query.evaluateExpressions(query, attributes, null));

        attributes.put("test", "path/");
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType(query));
        assertEquals("1", Query.evaluateExpressions(query, attributes, null));
        assertEquals("path", Query.evaluateExpressions("${test:getDelimitedField(1, '/')}", attributes, null));
    }

    @Test
    public void testAllAttributes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "1234");
        attributes.put("xyz", "4132");
        attributes.put("hello", "world!");

        verifyEquals("${allAttributes('abc', 'xyz'):matches('\\d+')}", attributes, true);
        verifyEquals("${allAttributes('abc', 'xyz'):toNumber():lt(99999)}", attributes, true);
        verifyEquals("${allAttributes('abc', 'hello'):length():gt(3)}", attributes, true);
        verifyEquals("${allAttributes('abc', 'hello'):length():equals(4)}", attributes, false);
        verifyEquals("${allAttributes('abc', 'xyz'):length():equals(4)}", attributes, true);
        verifyEquals("${allAttributes('abc', 'xyz', 'other'):isNull()}", attributes, false);

        try {
            Query.compile("${allAttributes('#ah'):equals('hello')");
            Assert.fail("Was able to compile with allAttributes and an invalid attribute name");
        } catch (final AttributeExpressionLanguageParsingException e) {
            // expected behavior
        }
    }

    @Test
    public void testMathWholeNumberOperators() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "1234");
        attributes.put("xyz", "4132");
        attributes.put("hello", "world!");

        verifyEquals("${xyz:toNumber():gt( ${abc:toNumber()} )}", attributes, true);
    }

    @Test
    public void testMathDecimalOperators() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("one", "1.1");
        attributes.put("two", "2.2");
        attributes.put("one_2", "1.1");

        verifyEquals("${one:lt(${two})}", attributes, true);
        verifyEquals("${one:lt(${one_2})}", attributes, false);
        verifyEquals("${two:lt(${one})}", attributes, false);

        verifyEquals("${one:le(${two})}", attributes, true);
        verifyEquals("${one:le(${one_2})}", attributes, true);
        verifyEquals("${two:le(${one_2})}", attributes, false);

        verifyEquals("${one:ge(${two})}", attributes, false);
        verifyEquals("${one:ge(${one_2})}", attributes, true);
        verifyEquals("${two:ge(${one_2})}", attributes, true);

        verifyEquals("${one:gt(${two})}", attributes, false);
        verifyEquals("${one:gt(${one_2})}", attributes, false);
        verifyEquals("${two:gt(${one})}", attributes, true);
    }

    @Test
    public void testMathNumberDecimalConversion() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xyz", "1.332");
        attributes.put("hello", "world!");

        verifyEquals("${xyz:toNumber()}", attributes, 1L);

        attributes.put("xyz", "2");
        attributes.put("hello", "world!");

        verifyEquals("${xyz:toDecimal()}", attributes, 2D);
    }

    @Test
    public void testLiteral() {
        final Map<String, String> attributes = new HashMap<>();

        verifyEquals("${literal(5)}", attributes, "5");

        verifyEquals("${literal(\"5\")}", attributes, "5");

        verifyEquals("${literal(5):toNumber()}", attributes, 5L);
        verifyEquals("${literal(5):toDecimal()}", attributes, 5D);

        verifyEquals("${literal(\"5.5\")}", attributes, "5.5");

        verifyEquals("${literal(5.5):toNumber()}", attributes, 5L);
        verifyEquals("${literal(-5.5):toNumber()}", attributes, -5L);
        verifyEquals("${literal(+5.5):toNumber()}", attributes, 5L);

        verifyEquals("${literal(5.5):toDecimal()}", attributes, 5.5D);
        verifyEquals("${literal(-5.5):toDecimal()}", attributes, -5.5D);
        verifyEquals("${literal(+5.5):toDecimal()}", attributes, 5.5D);

        verifyEquals("${literal('0xF.Fp10'):toDecimal()}", attributes, 0xF.Fp10D);

        verifyEquals("${literal('0x1234567890ABCDEF'):toNumber()}", attributes, 0x1234567890ABCDEFL);
        verifyEquals("${literal('-0x1234567890ABCDEF'):toNumber()}", attributes, -0x1234567890ABCDEFL);

        verifyEquals("${literal('-0x1234567890abcdef'):toNumber()}", attributes, -0x1234567890abcdefL);
        verifyEquals("${literal('0x1234567890abcdef'):toNumber()}", attributes, 0x1234567890abcdefL);
    }

    @Test
    public void testDecimalParsing() {
        final Map<String, String> attributes = new HashMap<>();

        // Test decimal format X.X
        verifyEquals("${literal(5.5):toDecimal()}", attributes, 5.5D);
        verifyEquals("${literal(-12.5):toDecimal()}", attributes, -12.5D);
        verifyEquals("${literal(+12.5):toDecimal()}", attributes, 12.5D);

        // Test decimal format X.XEX with positive exponent
        verifyEquals("${literal(-12.5E2):toDecimal()}", attributes, -12.5E2D);
        verifyEquals("${literal(-12.5e2):toDecimal()}", attributes, -12.5e2D);
        verifyEquals("${literal(-12.5e+2):toDecimal()}", attributes, -12.5e+2D);
        verifyEquals("${literal(12.5E+2):toDecimal()}", attributes, 12.5E+2D);
        verifyEquals("${literal(+12.5e+2):toDecimal()}", attributes, +12.5e+2D);
        verifyEquals("${literal(+12.5E2):toDecimal()}", attributes, +12.5E2D);
        verifyEquals("${literal(-12.5e2):toDecimal()}", attributes, -12.5e2D);
        verifyEquals("${literal(12.5E2):toDecimal()}", attributes, 12.5E2D);
        verifyEquals("${literal(+12.5e2):toDecimal()}", attributes, +12.5e2D);

        // Test decimal format X.XEX with negative exponent
        verifyEquals("${literal(-12.5E-2):toDecimal()}", attributes, -12.5E-2D);
        verifyEquals("${literal(12.5E-2):toDecimal()}", attributes, 12.5E-2D);
        verifyEquals("${literal(+12.5e-2):toDecimal()}", attributes, +12.5e-2D);

        // Test decimal format .X
        verifyEquals("${literal(.5):toDecimal()}", attributes, .5D);
        verifyEquals("${literal(.5):toDecimal()}", attributes, .5D);
        verifyEquals("${literal(-.5):toDecimal()}", attributes, -0.5D);
        verifyEquals("${literal(+.5):toDecimal()}", attributes, .5D);

        // Test decimal format .XEX with positive exponent
        verifyEquals("${literal(-.5E2):toDecimal()}", attributes, -.5E2D);
        verifyEquals("${literal(-.5E2):toDecimal()}", attributes, -.5E2D);
        verifyEquals("${literal(-.5e+2):toDecimal()}", attributes, -.5e+2D);
        verifyEquals("${literal(.5E+2):toDecimal()}", attributes, .5E+2D);
        verifyEquals("${literal(+.5e+2):toDecimal()}", attributes, +.5e+2D);
        verifyEquals("${literal(+.5E2):toDecimal()}", attributes, +.5E2D);
        verifyEquals("${literal(-.5e2):toDecimal()}", attributes, -.5e2D);
        verifyEquals("${literal(.5E2):toDecimal()}", attributes, .5E2D);
        verifyEquals("${literal(+.5e2):toDecimal()}", attributes, +.5e2D);

        // Test decimal format .XEX with negative exponent
        verifyEquals("${literal(-.5E-2):toDecimal()}", attributes, -.5E-2D);
        verifyEquals("${literal(.5e-2):toDecimal()}", attributes, .5e-2D);
        verifyEquals("${literal(+.5E-2):toDecimal()}", attributes, +.5E-2D);

        // Verify allowed values
        verifyEquals("${literal(9876543210.0123456789e123):toDecimal()}", attributes, 9876543210.0123456789e123D);

        verifyEmpty("${literal('A.1e123'):toDecimal()}", attributes);
        verifyEmpty("${literal('0.Ae123'):toDecimal()}", attributes);
        verifyEmpty("${literal('0.1eA'):toDecimal()}", attributes);

        // --------- Hex format ------//

        // Test Hex format X.
        verifyEquals("${literal('0xF1.p2'):toDecimal()}", attributes, 0xF1.p2D);
        verifyEquals("${literal('+0xF1.P2'):toDecimal()}", attributes, +0xF1.p2D);
        verifyEquals("${literal('-0xF1.p2'):toDecimal()}", attributes, -0xF1.p2D);

        // Test Hex format X.XEX with positive exponent
        verifyEquals("${literal('-0xF1.5Bp2'):toDecimal()}", attributes, -0xF1.5Bp2D);
        verifyEquals("${literal('-0xF1.5BP2'):toDecimal()}", attributes, -0xF1.5BP2D);
        verifyEquals("${literal('-0xF1.5BP+2'):toDecimal()}", attributes, -0xF1.5Bp+2D);
        verifyEquals("${literal('0xF1.5BP+2'):toDecimal()}", attributes, 0xF1.5BP+2D);
        verifyEquals("${literal('+0xF1.5Bp+2'):toDecimal()}", attributes, +0xF1.5Bp+2D);
        verifyEquals("${literal('+0xF1.5BP2'):toDecimal()}", attributes, +0xF1.5BP2D);
        verifyEquals("${literal('-0xF1.5Bp2'):toDecimal()}", attributes, -0xF1.5Bp2D);
        verifyEquals("${literal('0xF1.5BP2'):toDecimal()}", attributes, 0xF1.5BP2D);
        verifyEquals("${literal('+0xF1.5Bp2'):toDecimal()}", attributes, +0xF1.5Bp2D);

        // Test decimal format X.XEX with negative exponent
        verifyEquals("${literal('-0xF1.5BP-2'):toDecimal()}", attributes, -0xF1.5BP-2D);
        verifyEquals("${literal('0xF1.5BP-2'):toDecimal()}", attributes, 0xF1.5BP-2D);
        verifyEquals("${literal('+0xF1.5Bp-2'):toDecimal()}", attributes, +0xF1.5Bp-2D);

        // Test decimal format .XEX with positive exponent
        verifyEquals("${literal('0x.5BP0'):toDecimal()}", attributes, 0x.5BP0D);
        verifyEquals("${literal('-0x.5BP0'):toDecimal()}", attributes, -0x.5BP0D);
        verifyEquals("${literal('-0x.5BP+2'):toDecimal()}", attributes, -0x.5BP+2D);
        verifyEquals("${literal('0x.5BP+2'):toDecimal()}", attributes, 0x.5BP+2D);
        verifyEquals("${literal('+0x.5Bp+2'):toDecimal()}", attributes, +0x.5Bp+2D);
        verifyEquals("${literal('+0x.5BP2'):toDecimal()}", attributes, +0x.5BP2D);
        verifyEquals("${literal('-0x.5Bp2'):toDecimal()}", attributes, -0x.5Bp2D);
        verifyEquals("${literal('0x.5BP2'):toDecimal()}", attributes, 0x.5BP2D);
        verifyEquals("${literal('+0x.5Bp+2'):toDecimal()}", attributes, +0x.5Bp2D);

        // Test decimal format .XEX with negative exponent
        verifyEquals("${literal('-0x.5BP-2'):toDecimal()}", attributes, -0x.5BP-2D);
        verifyEquals("${literal('0x.5Bp-2'):toDecimal()}", attributes, 0x.5Bp-2D);
        verifyEquals("${literal('+0x.5BP-2'):toDecimal()}", attributes, +0x.5BP-2D);

        // Verify allowed values
        verifyEquals("${literal('0xFEDCBA9876543210.0123456789ABCDEFp123'):toDecimal()}", attributes, 0xFEDCBA9876543210.0123456789ABCDEFp123D);
        verifyEquals("${literal('0xfedcba9876543210.0123456789abcdefp123'):toDecimal()}", attributes, 0xfedcba9876543210.0123456789abcdefp123D);
        verifyEmpty("${literal('0xG.1p123'):toDecimal()}", attributes);
        verifyEmpty("${literal('0x1.Gp123'):toDecimal()}", attributes);
        verifyEmpty("${literal('0x1.1pA'):toDecimal()}", attributes);
        verifyEmpty("${literal('0x1.1'):toDecimal()}", attributes);

        // Special cases
        verifyEquals("${literal('" + Double.toString(POSITIVE_INFINITY) + "'):toDecimal():plus(1):plus(2)}", attributes, POSITIVE_INFINITY);
        verifyEquals("${literal('" + Double.toString(NEGATIVE_INFINITY) + "'):toDecimal():plus(1):plus(2)}", attributes, NEGATIVE_INFINITY);
        verifyEquals("${literal('" + Double.toString(NaN) + "'):toDecimal():plus(1):plus(2)}", attributes, NaN);
    }

    @Test
    public void testAllMatchingAttributes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "1234");
        attributes.put("xyz", "4132");
        attributes.put("hello", "world!");
        attributes.put("123.cba", "hell.o");

        System.out.println(printTree(Query.compile("${allMatchingAttributes('(abc|xyz)'):matches('\\\\d+')}").getTree()));

        verifyEquals("${'123.cba':matches('hell\\.o')}", attributes, true);
        verifyEquals("${allMatchingAttributes('123\\.cba'):equals('hell.o')}", attributes, true);
        verifyEquals("${allMatchingAttributes('(abc|xyz)'):matches('\\d+')}", attributes, true);
        verifyEquals("${allMatchingAttributes('[ax].*'):toNumber():lt(99999)}", attributes, true);
        verifyEquals("${allMatchingAttributes('hell.'):length():gt(3)}", attributes, true);

        verifyEquals("${allMatchingAttributes('123\\.cba'):equals('no')}", attributes, false);
    }

    @Test
    public void testMatches() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "1234xyz4321");
        attributes.put("end", "xyz");
        attributes.put("xyz", "4132");
        attributes.put("hello", "world!");
        attributes.put("dotted", "abc.xyz");

        final String evaluated = Query.evaluateExpressions("${abc:matches('1234${end}4321')}", attributes, null);
        assertEquals("true", evaluated);

        attributes.put("end", "888");
        final String secondEvaluation = Query.evaluateExpressions("${abc:matches('1234${end}4321')}", attributes, null);
        assertEquals("false", secondEvaluation);

        verifyEquals("${dotted:matches('abc\\.xyz')}", attributes, true);
    }

    @Test
    public void testFind() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "1234xyz4321");
        attributes.put("end", "xyz");
        attributes.put("xyz", "4132");
        attributes.put("hello", "world!");
        attributes.put("dotted", "abc.xyz");

        final String evaluated = Query.evaluateExpressions("${abc:find('1234${end}4321')}", attributes, null);
        assertEquals("true", evaluated);

        attributes.put("end", "888");

        final String secondEvaluation = Query.evaluateExpressions("${abc:find('${end}4321')}", attributes, null);
        assertEquals("false", secondEvaluation);

        verifyEquals("${dotted:find('\\.')}", attributes, true);
    }

    @Test
    public void testSubstringAfter() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "file-255");

        verifyEquals("${filename:substringAfter('')}", attributes, "file-255");
        verifyEquals("${filename:substringAfterLast('')}", attributes, "file-255");
        verifyEquals("${filename:substringBefore('')}", attributes, "file-255");
        verifyEquals("${filename:substringBeforeLast('')}", attributes, "file-255");
        verifyEquals("${filename:substringBefore('file')}", attributes, "");

        attributes.put("uri", "sftp://some.uri");
        verifyEquals("${uri:substringAfter('sftp')}", attributes, "://some.uri");
    }

    @Test
    public void testSubstringAfterLast() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "file-file-255");

        verifyEquals("${filename:substringAfterLast('file-')}", attributes, "255");
        verifyEquals("${filename:substringAfterLast('5')}", attributes, "");
        verifyEquals("${filename:substringAfterLast('x')}", attributes, "file-file-255");
    }

    @Test
    public void testSubstringBefore() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("something", "some {} or other");

        verifyEquals("${something:substringBefore('}')}", attributes, "some {");
    }

    @Test
    public void testSubstring() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "file-255");

        verifyEquals("${filename:substring(1, 2)}", attributes, "i");
        verifyEquals("${filename:substring(4)}", attributes, "-255");
    }

    @Test
    public void testToRadix() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "file-255");
        attributes.put("filename2", "file-99999");

        verifyEquals("${filename:substringAfter('-'):toNumber():toRadix(16):toUpper()}", attributes, "FF");
        verifyEquals("${filename:substringAfter('-'):toNumber():toRadix(16, 4):toUpper()}", attributes, "00FF");
        verifyEquals("${filename:substringAfter('-'):toNumber():toRadix(36, 3):toUpper()}", attributes, "073");
    }

    @Test
    public void testFromRadix() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("test1", "ABCDEF");
        attributes.put("test2", "123");

        verifyEquals("${test1:fromRadix(16)}", attributes, 0xABCDEFL);
        verifyEquals("${test2:fromRadix(4)}", attributes, 27L);
    }

    @Test
    public void testBase64Encode(){
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("userpass", "admin:admin");
        verifyEquals("${userpass:base64Encode()}", attributes, "YWRtaW46YWRtaW4=");

    }

    @Test
    public void testBase64Decode(){
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("userpassbase64", "YWRtaW46YWRtaW4=");
        verifyEquals("${userpassbase64:base64Decode()}", attributes, "admin:admin");

    }

    @Test
    public void testDateFormatConversion() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("blue", "20130917162643");
        verifyEquals("${blue:toDate('yyyyMMddHHmmss'):format(\"yyyy/MM/dd HH:mm:ss.SSS'Z'\")}", attributes, "2013/09/17 16:26:43.000Z");
    }

    @Test
    public void testDateFormatConversionWithTimeZone() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("blue", "20130917162643");
        verifyEquals("${blue:toDate('yyyyMMddHHmmss', 'GMT'):format(\"yyyy/MM/dd HH:mm:ss.SSS'Z'\", 'GMT')}", attributes, "2013/09/17 16:26:43.000Z");
        verifyEquals("${blue:toDate('yyyyMMddHHmmss', 'GMT'):format(\"yyyy/MM/dd HH:mm:ss.SSS'Z'\", 'Europe/Paris')}", attributes, "2013/09/17 18:26:43.000Z");
        verifyEquals("${blue:toDate('yyyyMMddHHmmss', 'GMT'):format(\"yyyy/MM/dd HH:mm:ss.SSS'Z'\", 'America/Los_Angeles')}", attributes, "2013/09/17 09:26:43.000Z");
    }

    @Test
    public void testNot() {
        verifyEquals("${ab:notNull():not()}", new HashMap<String, String>(), true);
    }

    @Test
    public void testAttributesWithSpaces() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ab", "abc");
        attributes.put("a  b", "abc");

        verifyEquals("${ab}", attributes, "abc");
        verifyEquals("${'a  b'}", attributes, "abc");
        verifyEquals("${'a b':replaceNull('')}", attributes, "");
    }

    @Test
    public void testOr() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename1", "xabc");
        attributes.put("filename2", "yabc");
        attributes.put("filename3", "abcxy");

        verifyEquals("${filename1:startsWith('x'):or(true)}", attributes, true);
        verifyEquals("${filename1:startsWith('x'):or( ${filename1:startsWith('y')} )}", attributes, true);
        verifyEquals("${filename2:startsWith('x'):or( ${filename2:startsWith('y')} )}", attributes, true);
        verifyEquals("${filename3:startsWith('x'):or( ${filename3:startsWith('y')} )}", attributes, false);
        verifyEquals("${filename1:startsWith('x'):or( ${filename2:startsWith('y')} )}", attributes, true);
        verifyEquals("${filename2:startsWith('x'):or( ${filename1:startsWith('y')} )}", attributes, false);
    }

    @Test
    public void testAnd() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename1", "xabc");
        attributes.put("filename2", "yabc");
        attributes.put("filename 3", "abcxy");

        verifyEquals("${filename1:startsWith('x'):and(true)}", attributes, true);
        verifyEquals("${filename1:startsWith('x') : and( false )}", attributes, false);
        verifyEquals("${filename1:startsWith('x'):and( ${filename1:startsWith('y')} )}", attributes, false);
        verifyEquals("${filename2:startsWith('x'):and( ${filename2:startsWith('y')} )}", attributes, false);
        verifyEquals("${filename3:startsWith('x'):and( ${filename3:startsWith('y')} )}", attributes, false);
        verifyEquals("${filename1:startsWith('x'):and( ${filename2:startsWith('y')} )}", attributes, true);
        verifyEquals("${filename2:startsWith('x'):and( ${filename1:startsWith('y')} )}", attributes, false);
        verifyEquals("${filename1:startsWith('x'):and( ${'filename 3':endsWith('y')} )}", attributes, true);
    }

    @Test
    public void testAndOrNot() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename1", "xabc");
        attributes.put("filename2", "yabc");
        attributes.put("filename 3", "abcxy");

        final String query
                = "${"
                + "     'non-existing':notNull():not():and("
                + // true AND (
                "     ${filename1:startsWith('y')"
                + // false
                "     :or("
                + // or
                "       ${ filename1:startsWith('x'):and(false) }"
                + // false
                "     ):or("
                + // or
                "       ${ filename2:endsWith('xxxx'):or( ${'filename 3':length():gt(1)} ) }"
                + // true )
                "     )}"
                + "     )"
                + "}";

        System.out.println(query);
        verifyEquals(query, attributes, true);
    }

    @Test
    public void testAndOrLogicWithAnyAll() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename1", "xabc");
        attributes.put("filename2", "yabc");
        attributes.put("filename 3", "abcxy");

        verifyEquals("${anyMatchingAttribute('filename.*'):contains('abc'):and( ${filename2:equals('yabc')} )}", attributes, true);
        verifyEquals("${anyMatchingAttribute('filename.*'):contains('abc'):and( ${filename2:equals('xabc')} )}", attributes, false);
        verifyEquals("${anyMatchingAttribute('filename.*'):contains('abc'):not():or( ${filename2:equals('yabc')} )}", attributes, true);
        verifyEquals("${anyMatchingAttribute('filename.*'):contains('abc'):not():or( ${filename2:equals('xabc')} )}", attributes, false);
    }

    @Test
    public void testKeywords() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("UUID", "123");
        verifyEquals("${ 'UUID':toNumber():equals(123) }", attributes, true);
    }

    @Test
    public void testEqualsNumber() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "123");
        verifyEquals("${ abc:toNumber():equals(123) }", attributes, true);
    }

    @Test
    public void testIn() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("myEnum", "JOHN");
        verifyEquals("${ myEnum:in('PAUL', 'JOHN', 'MIKE') }", attributes, true);
        verifyEquals("${ myEnum:in('RED', 'BLUE', 'GREEN') }", attributes, false);

        attributes.put("toReplace", "BLUE");
        verifyEquals("${ myEnum:in('RED', ${ toReplace:replace('BLUE', 'JOHN') }, 'GREEN') }", attributes, true);
    }

    @Test
    public void testSubjectAsEmbeddedExpressionWithSurroundChars() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("b", "x");
        attributes.put("abcxcba", "hello");

        final String evaluated = Query.evaluateExpressions("${ 'abc${b}cba':substring(0, 1) }", attributes, null);
        assertEquals("h", evaluated);
    }

    @Test
    public void testToNumberFunctionReturnsNumberType() {
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType("${header.size:toNumber()}"));
    }

    @Test
    public void testRandomFunctionReturnsNumberType() {
        assertEquals(ResultType.WHOLE_NUMBER, Query.getResultType("${random()}"));
    }

    @Test
    public void testAnyAttributeEmbedded() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "test1");
        attributes.put("b2", "2test");
        attributes.put("c3", "3test3");

        final String query = "${a1:equals('test1'):and( ${anyAttribute('a1','b2','c3'):contains('2')})}";
        verifyEquals(query, attributes, true);
    }

    @Test
    public void testEvaluateWithinCurlyBraces() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");

        final String query = "{ ${abc} }";
        final List<String> expressions = Query.extractExpressions(query);
        assertEquals(1, expressions.size());
        assertEquals("${abc}", expressions.get(0));
        assertEquals("{ xyz }", Query.evaluateExpressions(query, attributes));
    }

    @Test
    public void testGetStateValue() {
        final Map<String, String> stateValues = new HashMap<>();
        stateValues.put("abc", "xyz");
        stateValues.put("123", "qwe");
        stateValues.put("true", "asd");
        stateValues.put("iop", "098");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "iop");
        attributes.put("4321", "123");
        attributes.put("false", "bnm");

        String query = "${getStateValue('abc')}";
        verifyEquals(query, attributes, stateValues, "xyz");

        query = "${getStateValue(${'4321':toString()})}";
        verifyEquals(query, attributes, stateValues, "qwe");

        query = "${getStateValue(${literal(true):toString()})}";
        verifyEquals(query, attributes, stateValues, "asd");

        query = "${getStateValue(${abc}):equals('098')}";
        verifyEquals(query, attributes, stateValues, true);
    }

    @Test
    public void testLiteralFunction() {
        final Map<String, String> attrs = Collections.<String, String>emptyMap();
        verifyEquals("${literal(2):gt(1)}", attrs, true);
        verifyEquals("${literal('hello'):substring(0, 1):equals('h')}", attrs, true);
    }

    @Test
    public void testRandomFunction() {
        final Map<String, String> attrs = Collections.<String, String>emptyMap();
        final Long negOne = Long.valueOf(-1L);
        final HashSet<Long> results = new HashSet<>(100);
        for (int i = 0; i < results.size(); i++) {
            long result = (Long) getResult("${random()}", attrs).getValue();
            assertThat("random", result, greaterThan(negOne));
            assertEquals("duplicate random", true, results.add(result));
        }
    }

    QueryResult<?> getResult(String expr, Map<String, String> attrs) {
        final Query query = Query.compile(expr);
        final QueryResult<?> result = query.evaluate(attrs);
        return result;
    }

    @Test
    public void testFunctionAfterReduce() {
        // Cannot call gt(2) after count() because count() is a 'reducing function'
        // and must be the last function in an expression.
        assertFalse(Query.isValidExpression("${allMatchingAttributes('a.*'):contains('2'):count():gt(2)}"));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a.1", "245");
        attributes.put("a.2", "123");
        attributes.put("a.3", "732");
        attributes.put("a.4", "343");
        attributes.put("a.5", "553");

        final String endsWithCount = "${allMatchingAttributes('a.*'):contains('2'):count()}";
        assertTrue(Query.isValidExpression(endsWithCount));
        verifyEquals(endsWithCount, attributes, 3L);

        // in order to check if value is greater than 2, need to first evaluate the
        // 'aggregate' and 'reducing' functions as an inner expression. Then we can
        // use the literal() function to make the result of the inner expression the subject
        // of the function gt()
        final String usingLiteral = "${literal(" + endsWithCount + "):gt(2)}";
        assertTrue(Query.isValidExpression(usingLiteral));
        verifyEquals(usingLiteral, attributes, true);

        attributes.clear();
        attributes.put("a1", "123");
        attributes.put("a2", "321");
        verifyEquals("${allMatchingAttributes('a.*'):contains('2')}", attributes, true);
        verifyEquals("${allMatchingAttributes('a.*'):contains('2'):toUpper():equals('TRUE')}", attributes, true);
        verifyEquals("${allMatchingAttributes('a.*'):contains('2'):equals('true'):and( ${literal(true)} )}", attributes, true);
    }

    @Test
    public void testGetDelimitedField() {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("line", "Name, Age, Title");

        // Test "simple" case - comma separated with no quoted or escaped text
        verifyEquals("${line:getDelimitedField(1)}", attributes, "Name");
        verifyEquals("${line:getDelimitedField(1, ',')}", attributes, "Name");
        verifyEquals("${line:getDelimitedField(1, ',', '\"')}", attributes, "Name");
        verifyEquals("${line:getDelimitedField(1, ',', '\"', '\\\\')}", attributes, "Name");

        verifyEquals("${line:getDelimitedField(2)}", attributes, " Age");
        verifyEquals("${line:getDelimitedField(2, ',')}", attributes, " Age");
        verifyEquals("${line:getDelimitedField(2, ',', '\"')}", attributes, " Age");
        verifyEquals("${line:getDelimitedField(2, ',', '\"', '\\\\')}", attributes, " Age");

        verifyEquals("${line:getDelimitedField(3)}", attributes, " Title");
        verifyEquals("${line:getDelimitedField(3, ',')}", attributes, " Title");
        verifyEquals("${line:getDelimitedField(3, ',', '\"')}", attributes, " Title");
        verifyEquals("${line:getDelimitedField(3, ',', '\"', '\\\\')}", attributes, " Title");

        // test with a space in column
        attributes.put("line", "First Name, Age, Title");
        verifyEquals("${line:getDelimitedField(1)}", attributes, "First Name");
        verifyEquals("${line:getDelimitedField(1, ',')}", attributes, "First Name");
        verifyEquals("${line:getDelimitedField(1, ',', '\"')}", attributes, "First Name");
        verifyEquals("${line:getDelimitedField(1, ',', '\"', '\\\\')}", attributes, "First Name");

        // test quoted value
        attributes.put("line", "\"Name (Last, First)\", Age, Title");
        verifyEquals("${line:getDelimitedField(1)}", attributes, "\"Name (Last, First)\"");
        verifyEquals("${line:getDelimitedField(1, ',')}", attributes, "\"Name (Last, First)\"");
        verifyEquals("${line:getDelimitedField(1, ',', '\"')}", attributes, "\"Name (Last, First)\"");
        verifyEquals("${line:getDelimitedField(1, ',', '\"', '\\\\')}", attributes, "\"Name (Last, First)\"");

        // test non-standard quote char
        attributes.put("line", "_Name (Last, First)_, Age, Title");
        verifyEquals("${line:getDelimitedField(1)}", attributes, "_Name (Last");
        verifyEquals("${line:getDelimitedField(1, ',', '_')}", attributes, "_Name (Last, First)_");

        // test escape char
        attributes.put("line", "Name (Last\\, First), Age, Title");
        verifyEquals("${line:getDelimitedField(1)}", attributes, "Name (Last\\, First)");

        attributes.put("line", "Name (Last__, First), Age, Title");
        verifyEquals("${line:getDelimitedField(1, ',', '\"', '_')}", attributes, "Name (Last__");

        attributes.put("line", "Name (Last_, First), Age, Title");
        verifyEquals("${line:getDelimitedField(1, ',', '\"', '_')}", attributes, "Name (Last_, First)");

        // test escape for enclosing chars
        attributes.put("line", "\\\"Name (Last, First), Age, Title");
        verifyEquals("${line:getDelimitedField(1)}", attributes, "\\\"Name (Last");

        // get non existing field
        attributes.put("line", "Name, Age, Title");
        verifyEquals("${line:getDelimitedField(12)}", attributes, "");

        // test escape char within quotes
        attributes.put("line", "col 1, col 2, \"The First, Second, and \\\"Last\\\" Column\", Last");
        verifyEquals("${line:getDelimitedField(3):trim()}", attributes, "\"The First, Second, and \\\"Last\\\" Column\"");

        // test stripping chars
        attributes.put("line", "col 1, col 2, \"The First, Second, and \\\"Last\\\" Column\", Last");
        verifyEquals("${line:getDelimitedField(3, ',', '\"', '\\\\', true):trim()}", attributes, "The First, Second, and \"Last\" Column");

        attributes.put("line", "\"Jacobson, John\", 32, Mr.");
        verifyEquals("${line:getDelimitedField(2)}", attributes, " 32");
    }

    @Test
    public void testEscapeFunctions() {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("string", "making air \"QUOTES\".");
        verifyEquals("${string:escapeJson()}", attributes, "making air \\\"QUOTES\\\".");

        attributes.put("string", "M & M");
        verifyEquals("${string:escapeXml()}", attributes, "M &amp; M");

        attributes.put("string", "making air \"QUOTES\".");
        verifyEquals("${string:escapeCsv()}", attributes, "\"making air \"\"QUOTES\"\".\"");

        attributes.put("string", "special ¡");
        verifyEquals("${string:escapeHtml3()}", attributes, "special &iexcl;");

        attributes.put("string", "special ♣");
        verifyEquals("${string:escapeHtml4()}", attributes, "special &clubs;");
      }

    @Test
    public void testUnescapeFunctions() {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("string", "making air \\\"QUOTES\\\".");
        verifyEquals("${string:unescapeJson()}", attributes, "making air \"QUOTES\".");

        attributes.put("string", "M &amp; M");
        verifyEquals("${string:unescapeXml()}", attributes, "M & M");

        attributes.put("string", "\"making air \"\"QUOTES\"\".\"");
        verifyEquals("${string:unescapeCsv()}", attributes, "making air \"QUOTES\".");

        attributes.put("string", "special &iexcl;");
        verifyEquals("${string:unescapeHtml3()}", attributes, "special ¡");

        attributes.put("string", "special &clubs;");
        verifyEquals("${string:unescapeHtml4()}", attributes, "special ♣");
    }

    @Test
    public void testIfElse() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${attr:isNull():ifElse('a', 'b')}", attributes, "a");
        verifyEquals("${attr:ifElse('a', 'b')}", attributes, "b");
        attributes.put("attr", "hello");
        verifyEquals("${attr:isNull():ifElse('a', 'b')}", attributes, "b");
        verifyEquals("${attr:ifElse('a', 'b')}", attributes, "b");
        attributes.put("attr", "true");
        verifyEquals("${attr:ifElse('a', 'b')}", attributes, "a");

        verifyEquals("${attr2:isNull():ifElse('a', 'b')}", attributes, "a");
        verifyEquals("${literal(true):ifElse('a', 'b')}", attributes, "a");
        verifyEquals("${literal(true):ifElse(false, 'b')}", attributes, "false");
    }


    private void verifyEquals(final String expression, final Map<String, String> attributes, final Object expectedResult) {
        verifyEquals(expression,attributes, null, expectedResult);
    }

    private void verifyEquals(final String expression, final Map<String, String> attributes, final Map<String, String> stateValues, final Object expectedResult) {
        Query.validateExpression(expression, false);
        assertEquals(String.valueOf(expectedResult), Query.evaluateExpressions(expression, attributes, null, stateValues));

        final Query query = Query.compile(expression);
        final QueryResult<?> result = query.evaluate(attributes, stateValues);

        if (expectedResult instanceof Long) {
            if (ResultType.NUMBER.equals(result.getResultType())) {
                final Number resultNumber = ((NumberQueryResult) result).getValue();
                assertTrue(resultNumber instanceof Long);
            } else {
                assertEquals(ResultType.WHOLE_NUMBER, result.getResultType());
            }
        } else if(expectedResult instanceof Double) {
            if (ResultType.NUMBER.equals(result.getResultType())) {
                final Number resultNumber = ((NumberQueryResult) result).getValue();
                assertTrue(resultNumber instanceof Double);
            } else {
                assertEquals(ResultType.DECIMAL, result.getResultType());
            }
        } else if (expectedResult instanceof Boolean) {
            assertEquals(ResultType.BOOLEAN, result.getResultType());
        } else {
            assertEquals(ResultType.STRING, result.getResultType());
        }

        assertEquals(expectedResult, result.getValue());
    }

    private void verifyEmpty(final String expression, final Map<String, String> attributes) {
        Query.validateExpression(expression, false);
        assertEquals(String.valueOf(""), Query.evaluateExpressions(expression, attributes, null));
    }

    private String getResourceAsString(String resourceName) throws IOException {
        try (final Reader reader = new InputStreamReader(new BufferedInputStream(getClass().getResourceAsStream(resourceName)))) {
            int n = 0;
            char[] buf = new char[1024];
            StringBuilder sb = new StringBuilder();
            while (n != -1) {
                try {
                    n = reader.read(buf, 0, buf.length);
                } catch (IOException e) {
                    throw new RuntimeException("failed to read resource", e);
                }
                if (n > 0) {
                    sb.append(buf, 0, n);
                }
            }
            return sb.toString();
        }
    }
}
