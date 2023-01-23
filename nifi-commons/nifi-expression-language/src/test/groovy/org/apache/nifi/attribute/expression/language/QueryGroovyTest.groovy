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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotEquals

class QueryGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryGroovyTest.class)

    @BeforeAll
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {
        Query.metaClass.static = null

    }

    @Test
    void testReplaceShouldReplaceAllLiteralMatches() {
        // Arrange
        int n = 3
        final String ORIGINAL_VALUE = "Hello World"
        final Map<String, String> attributes = [
                single   : ORIGINAL_VALUE,
                repeating: [ORIGINAL_VALUE].multiply(n).join(" ")]
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
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assertEquals(EXPECTED_SINGLE_RESULT, replaceSingleResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceSingleResult.resultType)

        assertEquals(EXPECTED_REPEATING_RESULT, replaceRepeatingResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceRepeatingResult.resultType)
    }

    @Test
    void testReplaceFirstShouldOnlyReplaceFirstRegexMatch() {
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

        final String REPLACE_SINGLE_EXPRESSION = "\${single:replaceFirst('${REPLACE_ONLY_FIRST_PATTERN}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace single | ${REPLACE_SINGLE_EXPRESSION}")
        final String REPLACE_REPEATING_EXPRESSION = "\${repeating:replaceFirst('${REPLACE_ONLY_FIRST_PATTERN}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace repeating | ${REPLACE_REPEATING_EXPRESSION}")

        Query replaceSingleQuery = Query.compile(REPLACE_SINGLE_EXPRESSION)
        Query replaceRepeatingQuery = Query.compile(REPLACE_REPEATING_EXPRESSION)

        // Act
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assertEquals(EXPECTED_SINGLE_RESULT, replaceSingleResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceSingleResult.resultType)

        assertEquals(EXPECTED_REPEATING_RESULT, replaceRepeatingResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceRepeatingResult.resultType)
    }

    @Test
    void testReplaceFirstShouldOnlyReplaceFirstLiteralMatch() {
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

        final String REPLACE_LITERAL = ORIGINAL_VALUE

        final String REPLACE_SINGLE_EXPRESSION = "\${single:replaceFirst('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace single | ${REPLACE_SINGLE_EXPRESSION}")
        final String REPLACE_REPEATING_EXPRESSION = "\${repeating:replaceFirst('${REPLACE_LITERAL}', '${REPLACEMENT_VALUE}')}"
        logger.expression("Replace repeating | ${REPLACE_REPEATING_EXPRESSION}")

        Query replaceSingleQuery = Query.compile(REPLACE_SINGLE_EXPRESSION)
        Query replaceRepeatingQuery = Query.compile(REPLACE_REPEATING_EXPRESSION)

        // Act
        QueryResult<?> replaceSingleResult = replaceSingleQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace single result: ${replaceSingleResult.value}")

        QueryResult<?> replaceRepeatingResult = replaceRepeatingQuery.evaluate(new StandardEvaluationContext(attributes))
        logger.info("Replace repeating result: ${replaceRepeatingResult.value}")

        // Assert
        assertEquals(EXPECTED_SINGLE_RESULT, replaceSingleResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceSingleResult.resultType)

        assertEquals(EXPECTED_REPEATING_RESULT, replaceRepeatingResult.value)
        assertEquals(AttributeExpression.ResultType.STRING, replaceRepeatingResult.resultType)
    }

    @Test
    void testShouldDemonstrateDifferenceBetweenStringReplaceAndStringReplaceFirst() {
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
        assertNotEquals(EXPECTED_SINGLE_RESULT, replaceSingleResult)
        assertNotEquals(EXPECTED_REPEATING_RESULT, replaceRepeatingResult)

        assertEquals(EXPECTED_SINGLE_RESULT, replaceFirstSingleResult)
        assertEquals(EXPECTED_REPEATING_RESULT, replaceFirstRepeatingResult)
    }
}