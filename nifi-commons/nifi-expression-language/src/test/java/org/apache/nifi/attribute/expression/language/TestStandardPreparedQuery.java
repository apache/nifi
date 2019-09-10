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

import org.apache.nifi.parameter.ParameterLookup;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStandardPreparedQuery {

    @Test
    public void testSimpleReference() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "world");

        assertEquals("world", evaluate("${xx}", attrs));
        assertEquals("hello, world!", evaluate("hello, ${xx}!", attrs));
    }

    @Test
    public void testEmbeddedReference() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "yy");
        attrs.put("yy", "world");

        assertEquals("world", evaluate("${${xx}}", attrs));
    }

    @Test
    @Ignore("Intended for manual performance testing; should not be run in an automated environment")
    public void test10MIterations() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "world");

        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${xx}");
        final long start = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            assertEquals("world", prepared.evaluateExpressions(new StandardEvaluationContext(attrs), null));
        }
        final long nanos = System.nanoTime() - start;
        System.out.println(TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    @Test
    @Ignore("Takes too long")
    public void test10MIterationsWithQuery() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "world");

        final long start = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            assertEquals("world", Query.evaluateExpressions("${xx}", attrs, ParameterLookup.EMPTY));
        }
        final long nanos = System.nanoTime() - start;
        System.out.println(TimeUnit.NANOSECONDS.toMillis(nanos));

    }

    @Test
    public void testSeveralSequentialExpressions() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("audience", "World");
        attributes.put("comma", ",");
        attributes.put("question", " how are you?");
        assertEquals("Hello, World, how are you?!", evaluate("Hello, ${audience}${comma}${question}!", attributes));

    }

    @Test
    public void testPreparedQueryWithReducingFunctionAny() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("comma", ",");
        attributes.put("question", " how are you?");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${anyAttribute('comma', 'question'):matches('hello')}");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("audience", "bla");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("comma", "hello");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionAll() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("comma", ",");
        attributes.put("question", " how are you?");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allAttributes('comma', 'question'):matches('hello')}");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("comma", "hello");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("question", "hello");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionAnyMatching() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("comma", ",");
        attributes.put("question", " how are you?");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${anyMatchingAttribute('audi.*'):matches('hello')}");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("audience", "bla");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("auditorium", "hello");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionAllMatching() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("comma", "hello");
        attributes.put("question", "hello");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allMatchingAttributes('.*'):matches('hello')}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("audience", "bla");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.remove("audience");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionAnyDelineated() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("number_list", "1,2,3,4,5,6,7");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${anyDelineatedValue(${number_list}, ','):contains('5')}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("number_list", "1,2,3");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("number_list", "5");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionAllDelineated() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("word_list", "beach,bananas,web");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allDelineatedValues(${word_list}, ','):contains('b')}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("word_list", "beach,party,web");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("word_list", "bee");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionJoin() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        attributes.put("boat", "World!");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allAttributes('hello', 'boat'):join(' ')}");
        assertEquals("Hello World!", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("boat", "Friend.");
        assertEquals("Hello Friend.", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithReducingFunctionCount() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        attributes.put("boat", "World!");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allAttributes('hello', 'boat'):contains('e'):count()}");
        assertEquals("1", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("boat", "end");
        assertEquals("2", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithSelectingAnd() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        attributes.put("boat", "World!");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allAttributes('hello', 'boat'):isEmpty():not():and(${hello:contains('o')})}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("hello", "hi");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithAnd() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${hello:contains('H'):and(${hello:contains('o')})}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("hello", "Hell");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithSelectingOr() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        attributes.put("boat", "World!");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${allAttributes('hello', 'boat'):matches('strict'):or(${hello:contains('o')})}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("hello", "hi");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testPreparedQueryWithOr() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("hello", "Hello");
        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${hello:contains('X'):or(${hello:contains('o')})}");
        assertEquals("true", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
        attributes.put("hello", "Hell");
        assertEquals("false", prepared.evaluateExpressions(new StandardEvaluationContext(attributes), null));
    }

    @Test
    public void testVariableImpacted() {
        final Set<String> attr = new HashSet<>();
        attr.add("attr");

        final Set<String> attr2 = new HashSet<>();
        attr2.add("attr");
        attr2.add("attr2");

        final Set<String> abc = new HashSet<>();
        abc.add("a");
        abc.add("b");
        abc.add("c");

        assertTrue(Query.prepare("${attr}").getVariableImpact().isImpacted("attr"));
        assertFalse(Query.prepare("${attr}").getVariableImpact().isImpacted("attr2"));
        assertTrue(Query.prepare("${attr:trim():toUpper():equals('abc')}").getVariableImpact().isImpacted("attr"));

        assertFalse(Query.prepare("${anyAttribute('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("attr"));
        assertTrue(Query.prepare("${anyAttribute('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("a"));
        assertTrue(Query.prepare("${anyAttribute('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("b"));
        assertTrue(Query.prepare("${anyAttribute('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("c"));

        assertFalse(Query.prepare("${allAttributes('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("attr"));
        assertTrue(Query.prepare("${allAttributes('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("a"));
        assertTrue(Query.prepare("${allAttributes('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("b"));
        assertTrue(Query.prepare("${allAttributes('a', 'b', 'c'):equals('hello')}").getVariableImpact().isImpacted("c"));

        assertTrue(Query.prepare("${attr:equals('${attr2}')}").getVariableImpact().isImpacted("attr"));
        assertTrue(Query.prepare("${attr:equals('${attr2}')}").getVariableImpact().isImpacted("attr2"));
        assertFalse(Query.prepare("${attr:equals('${attr2}')}").getVariableImpact().isImpacted("attr3"));

        assertTrue(Query.prepare("${allMatchingAttributes('a.*'):equals('hello')}").getVariableImpact().isImpacted("attr"));
        assertTrue(Query.prepare("${anyMatchingAttribute('a.*'):equals('hello')}").getVariableImpact().isImpacted("attr"));
    }

    private String evaluate(final String query, final Map<String, String> attrs) {
        final String evaluated = ((StandardPreparedQuery) Query.prepare(query)).evaluateExpressions(new StandardEvaluationContext(attrs), null);
        return evaluated;
    }

}
