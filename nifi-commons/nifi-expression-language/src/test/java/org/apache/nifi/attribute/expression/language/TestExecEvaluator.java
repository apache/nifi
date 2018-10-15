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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;

public class TestExecEvaluator extends QueryTestBase{

    @Before
    public void init() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "./src/test/resources/udf/nifi.properties");
    }

    @Test(expected = AttributeExpressionLanguageException.class)
    public void testExecBadClassException() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${exec('org.org.org', 0, true)}",
                attributes, "java.lang.String ja-ja");
    }

    @Test
    public void testExecuteLiterals() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${exec('org.apache.nifi.attribute.expression.language.SimpleUDF', 'hello')}",
                attributes, "hello-hello");
    }

    @Test
    public void testExecuteExpressions() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${exec('org.apache.nifi.attribute.expression.language.SimpleUDF', ${literal('hello')})}",
                attributes, "hello-hello");
    }

    @Test
    public void testExecuteDefaultNoParams() {
        verifyEmpty("${exec('org.apache.nifi.attribute.expression.language.SimpleUDF')}", null);
    }
    @Test
    public void testExecuteDefaultConcat() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${exec('org.apache.nifi.attribute.expression.language.SimpleUDF', ${literal('hello')}, 0, 'world')}",
                attributes, "hello|0|world");
    }
    @Test
    public void testClassPath() {
        final Map<String, String> attributes = new HashMap<>();
        verifyEquals("${exec('org.apache.nifi.attribute.expression.language.test.SimpleMaskCreditCard', "
                + "'1234-5678-9012-3456')}",
                attributes, "xxxx-xxxx-xxxx-3456");
    }
}
