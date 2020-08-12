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
package org.apache.nifi.parameter;

import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestExpressionLanguageAgnosticParameterParser {

    @Test
    public void testProperReferences() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final ParameterTokenList references = parameterParser.parseTokens("#{foo}");

        for (final ParameterReference reference : references.toReferenceList()) {
            assertEquals("foo", reference.getParameterName());
            assertEquals(0, reference.getStartOffset());
            assertEquals(5, reference.getEndOffset());
            assertEquals("#{foo}", reference.getText());
        }

        List<ParameterReference> referenceList = parameterParser.parseTokens("/#{foo}").toReferenceList();
        assertEquals(1, referenceList.size());

        ParameterReference reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getText());

        referenceList = parameterParser.parseTokens("/#{foo}/").toReferenceList();
        assertEquals(1, referenceList.size());
        reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getText());

        referenceList = parameterParser.parseTokens("/#{foo}/#{bar}#{baz}").toReferenceList();
        assertEquals(3, referenceList.size());

        reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getText());

        reference = referenceList.get(1);
        assertEquals("bar", reference.getParameterName());
        assertEquals(8, reference.getStartOffset());
        assertEquals(13, reference.getEndOffset());
        assertEquals("#{bar}", reference.getText());

        reference = referenceList.get(2);
        assertEquals("baz", reference.getParameterName());
        assertEquals(14, reference.getStartOffset());
        assertEquals(19, reference.getEndOffset());
        assertEquals("#{baz}", reference.getText());
    }

    @Test
    public void testEscapeSequences() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        List<ParameterToken> tokenList = parameterParser.parseTokens("#{foo}").toList();
        assertEquals(1, tokenList.size());

        ParameterToken token = tokenList.get(0);
        assertTrue(token.isParameterReference());
        assertEquals("foo", ((ParameterReference) token).getParameterName());
        assertEquals(0, token.getStartOffset());
        assertEquals(5, token.getEndOffset());
        assertEquals("#{foo}", token.getText());
        assertFalse(token.isEscapeSequence());

        tokenList = parameterParser.parseTokens("##{foo}").toList();
        assertEquals(1, tokenList.size());

        token = tokenList.get(0);
        assertFalse(token.isParameterReference());
        assertEquals(0, token.getStartOffset());
        assertEquals(6, token.getEndOffset());
        assertEquals("##{foo}", token.getText());
        assertTrue(token.isEscapeSequence());

        tokenList = parameterParser.parseTokens("###{foo}").toList();
        assertEquals(2, tokenList.size());

        token = tokenList.get(0);
        assertFalse(token.isParameterReference());
        assertEquals(0, token.getStartOffset());
        assertEquals(1, token.getEndOffset());
        assertEquals("##", token.getText());
        assertTrue(token.isEscapeSequence());

        token = tokenList.get(1);
        assertTrue(token.isParameterReference());
        assertEquals("foo", ((ParameterReference) token).getParameterName());
        assertEquals(2, token.getStartOffset());
        assertEquals(7, token.getEndOffset());
        assertFalse(token.isEscapeSequence());

        // Test an escaped # followed by an escaped #{foo}
        tokenList = parameterParser.parseTokens("####{foo}").toList();
        assertEquals(2, tokenList.size());

        token = tokenList.get(0);
        assertFalse(token.isParameterReference());
        assertEquals(0, token.getStartOffset());
        assertEquals(1, token.getEndOffset());
        assertEquals("##", token.getText());
        assertTrue(token.isEscapeSequence());

        token = tokenList.get(1);
        assertFalse(token.isParameterReference());
        assertEquals(2, token.getStartOffset());
        assertEquals(8, token.getEndOffset());
        assertTrue(token.isEscapeSequence());

        // Test multiple escaped # followed by a reference of #{foo}
        tokenList = parameterParser.parseTokens("#####{foo}").toList();
        assertEquals(3, tokenList.size());

        token = tokenList.get(0);
        assertFalse(token.isParameterReference());
        assertEquals(0, token.getStartOffset());
        assertEquals(1, token.getEndOffset());
        assertEquals("##", token.getText());
        assertTrue(token.isEscapeSequence());

        token = tokenList.get(1);
        assertFalse(token.isParameterReference());
        assertEquals(2, token.getStartOffset());
        assertEquals(3, token.getEndOffset());
        assertEquals("##", token.getText());
        assertTrue(token.isEscapeSequence());

        token = tokenList.get(2);
        assertTrue(token.isParameterReference());
        assertEquals("foo", ((ParameterReference) token).getParameterName());
        assertEquals(4, token.getStartOffset());
        assertEquals(9, token.getEndOffset());
        assertFalse(token.isEscapeSequence());
    }

    @Test
    public void testNonReferences() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        for (final String input : new String[] {"#foo", "Some text #{blah foo", "#foo}", "#}foo{", "#f{oo}", "#", "##", "###", "####", "#####", "#{", "##{", "###{"}) {
            assertEquals(0, parameterParser.parseTokens(input).toList().size());
        }
    }

    @Test
    public void testReferenceWithinExpressionLanguage() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final List<ParameterToken> tokens = parameterParser.parseTokens("${#{hello}:toUpper()}").toList();
        assertEquals(1, tokens.size());
    }

    @Test
    public void testReferenceInsideAndOutsideExpressionLanguage() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final List<ParameterToken> tokens = parameterParser.parseTokens("#{hello}${#{hello}:toUpper()}#{hello}").toList();
        assertEquals(3, tokens.size());

        for (final ParameterToken token : tokens) {
            assertEquals("hello", ((ParameterReference) token).getParameterName());
        }
    }

    @Test
    public void testMultipleReferencesDifferentExpressions() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final List<ParameterToken> tokens = parameterParser.parseTokens("${#{hello}}${#{there}}").toList();
        assertEquals(2, tokens.size());

        final ParameterToken firstToken = tokens.get(0);
        assertTrue(firstToken.isParameterReference());
        assertEquals("hello", ((ParameterReference) firstToken).getParameterName());

        final ParameterToken secondToken = tokens.get(1);
        assertTrue(secondToken.isParameterReference());
        assertEquals("there", ((ParameterReference) secondToken).getParameterName());
    }

    @Test
    public void testMultipleReferencesSameExpression() {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final List<ParameterToken> tokens = parameterParser.parseTokens("${#{hello}:append(#{there})}").toList();
        assertEquals(2, tokens.size());

        final ParameterToken firstToken = tokens.get(0);
        assertTrue(firstToken.isParameterReference());
        assertEquals("hello", ((ParameterReference) firstToken).getParameterName());

        final ParameterToken secondToken = tokens.get(1);
        assertTrue(secondToken.isParameterReference());
        assertEquals("there", ((ParameterReference) secondToken).getParameterName());
    }

}
