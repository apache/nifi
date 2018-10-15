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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.junit.Assert;

public class QueryTestBase {

    protected void assertValid(final String query) {
        try {
            Query.compile(query);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Expected query to be valid, but it failed to compile due to " + e);
        }
    }

    protected void assertInvalid(final String query) {
        try {
            Query.compile(query);
            Assert.fail("Expected query to be invalid, but it did compile");
        } catch (final Exception e) {
        }
    }

    protected String printTree(final Tree tree) {
        final StringBuilder sb = new StringBuilder();
        printTree(tree, 0, sb);

        return sb.toString();
    }

    protected void printTree(final Tree tree, final int spaces, final StringBuilder sb) {
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

    protected void verifyEquals(final String expression, final Map<String, String> attributes, final Object expectedResult) {
        verifyEquals(expression,attributes, null, expectedResult);
    }

    protected void verifyEquals(final String expression, final Map<String, String> attributes, final Map<String, String> stateValues, final Object expectedResult) {
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

    protected void verifyEmpty(final String expression, final Map<String, String> attributes) {
        Query.validateExpression(expression, false);
        assertEquals(String.valueOf(""), Query.evaluateExpressions(expression, attributes, null));
    }

    protected String getResourceAsString(String resourceName) throws IOException {
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
