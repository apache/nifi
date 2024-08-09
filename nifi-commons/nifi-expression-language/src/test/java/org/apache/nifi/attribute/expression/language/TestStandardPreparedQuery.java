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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardPreparedQuery {

    private static final Logger logger = LoggerFactory.getLogger(TestStandardPreparedQuery.class);

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
    @Disabled("Intended for manual performance testing; should not be run in an automated environment")
    public void test10MIterations() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "world");

        final StandardPreparedQuery prepared = (StandardPreparedQuery) Query.prepare("${xx}");
        final long start = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            assertEquals("world", prepared.evaluateExpressions(new StandardEvaluationContext(attrs), null));
        }
        final long nanos = System.nanoTime() - start;
        logger.info("{}", TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    @Test
    @Disabled("Takes too long")
    public void test10MIterationsWithQuery() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("xx", "world");

        final long start = System.nanoTime();
        for (int i = 0; i < 10000000; i++) {
            assertEquals("world", Query.evaluateExpressions("${xx}", attrs, ParameterLookup.EMPTY));
        }
        final long nanos = System.nanoTime() - start;
        logger.info("{}", TimeUnit.NANOSECONDS.toMillis(nanos));
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
    public void testSensitiveParameter() {
        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("param", new Parameter.Builder().name("param").value("value").build());
        parameters.put("sensi", new Parameter.Builder().name("sensi").sensitive(true).value("secret").build());

        final ParameterLookup parameterLookup = new ParameterLookup() {
            @Override
            public Optional<Parameter> getParameter(final String parameterName) {
                return Optional.ofNullable(parameters.get(parameterName));
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return parameters.isEmpty();
            }
        };

        final String value = Query.prepare("#{param}").evaluateExpressions(new StandardEvaluationContext(Collections.emptyMap(), Collections.emptyMap(), parameterLookup), null);
        assertEquals("value", value);

        final String secret = Query.prepare("#{sensi}").evaluateExpressions(new StandardEvaluationContext(Collections.emptyMap(), Collections.emptyMap(), parameterLookup), null);
        assertEquals("secret", secret);

        final String invalid = Query.prepare("${#{sensi}}").evaluateExpressions(new StandardEvaluationContext(Collections.emptyMap(), Collections.emptyMap(), parameterLookup), null);
        assertEquals("", invalid);
    }

    @Test
    public void testEvaluateExpressionLanguageVariableValueSensitiveParameterReference() {
        final String parameterName = "protected";
        final String parameterValue = "secret";

        final ParameterLookup parameterLookup = mock(ParameterLookup.class);
        final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(parameterName).sensitive(true).build();
        final Parameter parameter = new Parameter.Builder().descriptor(parameterDescriptor).value(parameterValue).build();
        when(parameterLookup.getParameter(eq(parameterName))).thenReturn(Optional.of(parameter));
        when(parameterLookup.isEmpty()).thenReturn(false);

        final Map<String, String> variables = new LinkedHashMap<>();
        final String variableName = "variable";
        final String variableValue = String.format("#{%s}", parameterName);
        variables.put(variableName, variableValue);

        final StandardEvaluationContext context = new StandardEvaluationContext(variables, Collections.emptyMap(), parameterLookup);

        final String queryExpression = String.format("${%s:evaluateELString()}", variableName);
        final String value = Query.prepare(queryExpression).evaluateExpressions(context, null);

        assertNotEquals(parameterValue, value);
    }

    @Test
    public void testVariableImpacted() {
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

    @Test
    public void testIsExpressionLanguagePresent() {
        assertFalse(Query.prepare("value").isExpressionLanguagePresent());
        assertFalse(Query.prepare("").isExpressionLanguagePresent());

        assertTrue(Query.prepare("${variable}").isExpressionLanguagePresent());
        assertTrue(Query.prepare("${hostname()}").isExpressionLanguagePresent());
        assertTrue(Query.prepare("${hostname():equals('localhost')}").isExpressionLanguagePresent());
        assertTrue(Query.prepare("prefix-${hostname()}").isExpressionLanguagePresent());
        assertTrue(Query.prepare("${hostname()}-suffix").isExpressionLanguagePresent());
        assertTrue(Query.prepare("${variable1}${hostname()}${variable2}").isExpressionLanguagePresent());
        assertTrue(Query.prepare("${${variable}}").isExpressionLanguagePresent());

        assertFalse(Query.prepare("${}").isExpressionLanguagePresent());

        assertFalse(Query.prepare("#{param}").isExpressionLanguagePresent());
    }

    private String evaluate(final String query, final Map<String, String> attrs) {
        return Query.prepare(query).evaluateExpressions(new StandardEvaluationContext(attrs), null);
    }

}
