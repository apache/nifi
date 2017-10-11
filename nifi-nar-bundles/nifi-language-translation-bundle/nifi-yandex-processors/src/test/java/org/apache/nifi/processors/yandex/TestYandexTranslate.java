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
package org.apache.nifi.processors.yandex;

import org.apache.nifi.processors.yandex.model.Translation;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.Response.StatusType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestYandexTranslate {

    private static final Map<String, String> translations = new HashMap<>();

    @BeforeClass
    public static void setupTranslationMap() {
        translations.put("bonjour", "hello");
        translations.put("traduire", "translate");
        translations.put("amusant", "fun");
        translations.put("ordinateur", "computer");
    }

    private TestRunner createTestRunner(final int statusCode) {
        return TestRunners.newTestRunner(new YandexTranslate() {
            @Override
            protected Invocation prepareResource(final String key, final List<String> text, final String sourceLanguage, final String destLanguage) {
                final Invocation invocation = Mockito.mock(Invocation.class);

                Mockito.doAnswer(new Answer<Response>() {
                    @Override
                    public Response answer(final InvocationOnMock invocation) throws Throwable {
                        final Response response = Mockito.mock(Response.class);

                        final StatusType statusType = new StatusType() {
                            @Override
                            public int getStatusCode() {
                                return statusCode;
                            }

                            @Override
                            public String getReasonPhrase() {
                                return String.valueOf(statusCode);
                            }

                            @Override
                            public Family getFamily() {
                                return statusCode == 200 ? Family.SUCCESSFUL : Family.SERVER_ERROR;
                            }
                        };

                        Mockito.when(response.getStatus()).thenReturn(statusCode);
                        Mockito.when(response.getStatusInfo()).thenReturn(statusType);

                        if (statusCode == 200) {
                            final Translation translation = new Translation();
                            translation.setCode(statusCode);
                            translation.setLang(destLanguage);

                            final List<String> translationList = new ArrayList<>();
                            for (final String original : text) {
                                final String translated = translations.get(original);
                                translationList.add(translated == null ? original : translated);
                            }

                            translation.setText(translationList);

                            Mockito.when(response.readEntity(Translation.class)).thenReturn(translation);
                        }

                        return response;
                    }
                }).when(invocation).invoke();
                return invocation;
            }
        });
    }

    @Test
    public void testTranslateContent() {
        final TestRunner testRunner = createTestRunner(200);
        testRunner.setProperty(YandexTranslate.KEY, "a");
        testRunner.setProperty(YandexTranslate.SOURCE_LANGUAGE, "fr");
        testRunner.setProperty(YandexTranslate.TARGET_LANGUAGE, "en");
        testRunner.setProperty(YandexTranslate.TRANSLATE_CONTENT, "true");
        testRunner.setProperty(YandexTranslate.CHARACTER_SET, "UTF-8");

        testRunner.enqueue("bonjour".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(YandexTranslate.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(YandexTranslate.REL_SUCCESS).get(0);

        final String outText = new String(out.toByteArray());
        assertEquals("hello", outText);
    }

    @Test
    public void testTranslateSingleAttribute() {
        final TestRunner testRunner = createTestRunner(200);

        testRunner.setProperty(YandexTranslate.KEY, "A");
        testRunner.setProperty(YandexTranslate.SOURCE_LANGUAGE, "fr");
        testRunner.setProperty(YandexTranslate.TARGET_LANGUAGE, "en");
        testRunner.setProperty(YandexTranslate.TRANSLATE_CONTENT, "false");
        testRunner.setProperty(YandexTranslate.CHARACTER_SET, "UTF-8");
        testRunner.setProperty("translated", "bonjour");

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(YandexTranslate.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(YandexTranslate.REL_SUCCESS).get(0);

        assertEquals(0, out.toByteArray().length);
        out.assertAttributeEquals("translated", "hello");
    }

    @Test
    public void testTranslateMultipleAttributes() {
        final TestRunner testRunner = createTestRunner(200);

        testRunner.setProperty(YandexTranslate.KEY, "A");
        testRunner.setProperty(YandexTranslate.SOURCE_LANGUAGE, "fr");
        testRunner.setProperty(YandexTranslate.TARGET_LANGUAGE, "en");
        testRunner.setProperty(YandexTranslate.TRANSLATE_CONTENT, "false");
        testRunner.setProperty(YandexTranslate.CHARACTER_SET, "UTF-8");
        testRunner.setProperty("hello", "bonjour");
        testRunner.setProperty("translate", "traduire");
        testRunner.setProperty("fun", "amusant");

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(YandexTranslate.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(YandexTranslate.REL_SUCCESS).get(0);

        assertEquals(0, out.toByteArray().length);
        out.assertAttributeEquals("hello", "hello");
        out.assertAttributeEquals("translate", "translate");
        out.assertAttributeEquals("fun", "fun");
    }

    @Test
    public void testTranslateContentAndMultipleAttributes() {
        final TestRunner testRunner = createTestRunner(200);

        testRunner.setProperty(YandexTranslate.KEY, "A");
        testRunner.setProperty(YandexTranslate.SOURCE_LANGUAGE, "fr");
        testRunner.setProperty(YandexTranslate.TARGET_LANGUAGE, "en");
        testRunner.setProperty(YandexTranslate.TRANSLATE_CONTENT, "true");
        testRunner.setProperty(YandexTranslate.CHARACTER_SET, "UTF-8");
        testRunner.setProperty("hello", "bonjour");
        testRunner.setProperty("translate", "traduire");
        testRunner.setProperty("fun", "amusant");
        testRunner.setProperty("nifi", "nifi");

        testRunner.enqueue("ordinateur".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(YandexTranslate.REL_SUCCESS, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(YandexTranslate.REL_SUCCESS).get(0);

        out.assertContentEquals("computer");

        out.assertAttributeEquals("hello", "hello");
        out.assertAttributeEquals("translate", "translate");
        out.assertAttributeEquals("fun", "fun");
        out.assertAttributeEquals("nifi", "nifi");
    }

    @Test
    public void testFailureResponse() {
        final TestRunner testRunner = createTestRunner(403);

        testRunner.setProperty(YandexTranslate.KEY, "A");
        testRunner.setProperty(YandexTranslate.SOURCE_LANGUAGE, "fr");
        testRunner.setProperty(YandexTranslate.TARGET_LANGUAGE, "en");
        testRunner.setProperty(YandexTranslate.TRANSLATE_CONTENT, "true");
        testRunner.setProperty(YandexTranslate.CHARACTER_SET, "UTF-8");
        testRunner.setProperty("hello", "bonjour");
        testRunner.setProperty("translate", "traduire");
        testRunner.setProperty("fun", "amusant");
        testRunner.setProperty("nifi", "nifi");

        testRunner.enqueue("ordinateur".getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(YandexTranslate.REL_TRANSLATION_FAILED, 1);
    }

}
