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
package org.apache.nifi.documentation.html;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.junit.Assert;

/**
 * A helper class to validate xml documents.
 *
 *
 */
public class XmlValidator {
    private static final String DOCTYPE = "<!DOCTYPE html>";

    private static final String EMPTY = "";

    /**
     * Asserts a failure if the provided XHTML is not valid
     *
     * @param xml the XML to validate
     */
    public static void assertXmlValid(String xml) {
        final String html = xml.replace(DOCTYPE, EMPTY);
        try {
            final DocumentProvider provider = new StandardDocumentProvider();
            provider.parse(new ByteArrayInputStream(html.getBytes(StandardCharsets.UTF_8)));
        } catch (final ProcessingException e) {
            Assert.fail(e.getMessage());
        }
    }

    public static void assertContains(String original, String subword) {
        Assert.assertTrue(original + " did not contain: " + subword, original.contains(subword));
    }

    public static void assertNotContains(String original, String subword) {
        Assert.assertFalse(original + " did contain: " + subword, original.contains(subword));
    }
}
