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
package org.apache.nifi.attribute.expression.language

import org.apache.nifi.attribute.expression.language.evaluation.QueryResult
import org.apache.nifi.expression.AttributeExpression
import org.apache.nifi.registry.VariableRegistry
import org.apache.nifi.registry.VariableRegistryFactory
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
public class QueryGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(QueryGroovyTest.class)

    @BeforeClass
    public static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
        Query.metaClass.static = null

    }

    @Test
    public void testReplaceShouldReplaceAllLiteralMatches() {
        // Arrange
        int n = 3
        final String ORIGINAL_VALUE = "Hello World"
        final Map<String, String> attributes = [
                single   : ORIGINAL_VALUE,
                repeating: [ORIGINAL_VALUE].multiply(n).join(" ")]
        final VariableRegistry variableRegistry = VariableRegistryFactory.getInstance(attributes)
        logger.info("Attributes: ${attributes}")

        final String REPLACEMENT_VALUE = "Goodbye Planet"

        final String EXPECTED_SINGLE_RESULT = REPLACEMENT_VALUE
        final String EXPECTED_REPEATING_RESULT = [REPLACEMENT_VALUE].multiply(n).join(" ")

        final String REPLACE_LITERAL = ORIGINAL_VALUE

        final String REPLACE_SINGLE_EXPRESSION = "\${single:replace('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace single | ${REPLACE_SINGLE_EXPRESSION}")
        final String REPLACE_REPEATING_EXPRESSION = "\${repeating:replace('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace repeating | ${REPLACE_REPEATING_EXPRESSION}")

        Query replaceSingleQuery = Query.compile(REPLACE_SINGLE_EXPRESSION)
        Query replaceRepeatingQuery = Query.compile(REPLACE_REPEATING_EXPRESSION)

        // Act
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(variableRegistry)
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(variableRegistry)
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assert replaceSingleResult.value == EXPECTED_SINGLE_RESULT
        assert replaceSingleResult.resultType == AttributeExpression.ResultType.STRING

        assert replaceRepeatingResult.value == EXPECTED_REPEATING_RESULT
        assert replaceRepeatingResult.resultType == AttributeExpression.ResultType.STRING
    }

    @Test
    public void testReplaceFirstShouldOnlyReplaceFirstRegexMatch() {
        // Arrange
        int n = 3
        final String ORIGINAL_VALUE = "Hello World"
        final Map<String, String> attributes = [
                single   : ORIGINAL_VALUE,
                repeating: [ORIGINAL_VALUE].multiply(n).join(" ")]
        final VariableRegistry variableRegistry = VariableRegistryFactory.getInstance(attributes)
        logger.info("Attributes: ${attributes}")

        final String REPLACEMENT_VALUE = "Goodbye Planet"

        final String EXPECTED_SINGLE_RESULT = REPLACEMENT_VALUE
        final String EXPECTED_REPEATING_RESULT = [REPLACEMENT_VALUE, [ORIGINAL_VALUE].multiply(n - 1)].flatten().join(" ")

        final String REPLACE_ONLY_FIRST_PATTERN = /\w+\s\w+\b??/

        final String REPLACE_SINGLE_EXPRESSION = "\${single:replaceFirst('${REPLACE_ONLY_FIRST_PATTERN}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace single | ${REPLACE_SINGLE_EXPRESSION}")
        final String REPLACE_REPEATING_EXPRESSION = "\${repeating:replaceFirst('${REPLACE_ONLY_FIRST_PATTERN}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace repeating | ${REPLACE_REPEATING_EXPRESSION}")

        Query replaceSingleQuery = Query.compile(REPLACE_SINGLE_EXPRESSION)
        Query replaceRepeatingQuery = Query.compile(REPLACE_REPEATING_EXPRESSION)

        // Act
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(variableRegistry)
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(variableRegistry)
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assert replaceSingleResult.value == EXPECTED_SINGLE_RESULT
        assert replaceSingleResult.resultType == AttributeExpression.ResultType.STRING

        assert replaceRepeatingResult.value == EXPECTED_REPEATING_RESULT
        assert replaceRepeatingResult.resultType == AttributeExpression.ResultType.STRING
    }

    @Test
    public void testReplaceFirstShouldOnlyReplaceFirstLiteralMatch() {
        // Arrange
        int n = 3
        final String ORIGINAL_VALUE = "Hello World"
        final Map<String, String> attributes = [
                single   : ORIGINAL_VALUE,
                repeating: [ORIGINAL_VALUE].multiply(n).join(" ")]
        final VariableRegistry variableRegistry = VariableRegistryFactory.getInstance(attributes)
        logger.info("Attributes: ${attributes}")

        final String REPLACEMENT_VALUE = "Goodbye Planet"

        final String EXPECTED_SINGLE_RESULT = REPLACEMENT_VALUE
        final String EXPECTED_REPEATING_RESULT = [REPLACEMENT_VALUE, [ORIGINAL_VALUE].multiply(n - 1)].flatten().join(" ")

        final String REPLACE_LITERAL = ORIGINAL_VALUE

        final String REPLACE_SINGLE_EXPRESSION = "\${single:replaceFirst('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace single | ${REPLACE_SINGLE_EXPRESSION}")
        final String REPLACE_REPEATING_EXPRESSION = "\${repeating:replaceFirst('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace repeating | ${REPLACE_REPEATING_EXPRESSION}")

        Query replaceSingleQuery = Query.compile(REPLACE_SINGLE_EXPRESSION)
        Query replaceRepeatingQuery = Query.compile(REPLACE_REPEATING_EXPRESSION)

        // Act
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(variableRegistry)
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(variableRegistry)
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assert replaceSingleResult.value == EXPECTED_SINGLE_RESULT
        assert replaceSingleResult.resultType == AttributeExpression.ResultType.STRING

        assert replaceRepeatingResult.value == EXPECTED_REPEATING_RESULT
        assert replaceRepeatingResult.resultType == AttributeExpression.ResultType.STRING
    }

    @Test
    public void testShouldDemonstrateDifferenceBetweenStringReplaceAndStringReplaceFirst() {
        // Arrange
        int n = 3
        final String ORIGINAL_VALUE = "Hello World"
        final Map<String, String> attributes = [
                single   : ORIGINAL_VALUE,
                repeating: [ORIGINAL_VALUE].multiply(n).join(" ")]
        logger.info("Attributes: ${attributes}")

        final String REPLACEMENT_VALUE = "Goodbye Planet"

        final String EXPECTED_SINGLE_RESULT = REPLACEMENT_VALUE
        final String EXPECTED_REPEATING_RESULT = [REPLACEMENT_VALUE, [ORIGINAL_VALUE].multiply(n - 1)].flatten().join(" ")

        final String REPLACE_ONLY_FIRST_PATTERN = /\w+\s\w+\b??/

        // Act
        
        // Execute on both single and repeating with String#replace()
        String replaceSingleResult = attributes.single.replace(REPLACE_ONLY_FIRST_PATTERN, REPLACEMENT_VALUE)
        logger.info("Replace single result: ${replaceSingleResult}")

        String replaceRepeatingResult = attributes.repeating.replace(REPLACE_ONLY_FIRST_PATTERN, REPLACEMENT_VALUE)
        logger.info("Replace repeating result: ${replaceRepeatingResult}")

        // Execute on both single and repeating with String#replaceFirst()
        String replaceFirstSingleResult = attributes.single.replaceFirst(REPLACE_ONLY_FIRST_PATTERN, REPLACEMENT_VALUE)
        logger.info("Replace first single result: ${replaceFirstSingleResult}")

        String replaceFirstRepeatingResult = attributes.repeating.replaceFirst(REPLACE_ONLY_FIRST_PATTERN, REPLACEMENT_VALUE)
        logger.info("Replace repeating result: ${replaceFirstRepeatingResult}")

        // Assert
        assert replaceSingleResult != EXPECTED_SINGLE_RESULT
        assert replaceRepeatingResult != EXPECTED_REPEATING_RESULT

        assert replaceFirstSingleResult == EXPECTED_SINGLE_RESULT
        assert replaceFirstRepeatingResult == EXPECTED_REPEATING_RESULT
    }
}