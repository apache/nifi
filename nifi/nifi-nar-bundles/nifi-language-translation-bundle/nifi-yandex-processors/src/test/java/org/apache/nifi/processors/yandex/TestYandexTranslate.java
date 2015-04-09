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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.nifi.processors.yandex.YandexTranslate;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only; requires local file to be populated with Yandex API Key")
public class TestYandexTranslate {

    private TestRunner testRunner;
    private String apiKey;

    @Before
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(YandexTranslate.class);
        
        final Properties properties = new Properties();
        try (final InputStream in = new FileInputStream(new File("C:/dev/notes/yandex-info.txt"))) {
        	properties.load(in);
        }
        apiKey = properties.getProperty("api_key").trim();
    }

    
    @Test
    public void testTranslateContent() {
    	testRunner.setProperty(YandexTranslate.KEY, apiKey);
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
    	testRunner.setProperty(YandexTranslate.KEY, apiKey);
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
    	testRunner.setProperty(YandexTranslate.KEY, apiKey);
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
    	testRunner.setProperty(YandexTranslate.KEY, apiKey);
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

}
