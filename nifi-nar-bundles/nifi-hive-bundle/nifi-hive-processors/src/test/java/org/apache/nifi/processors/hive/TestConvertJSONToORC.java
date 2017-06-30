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
package org.apache.nifi.processors.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.List;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.List;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class TestConvertJSONToORC {
    private TestRunner runner;

    public static final String JSON_EMPTY_CONTENT = "";
    public static final String JSON_CONTENT = "{\"key\" :  1 , \"data\" :  \"tom\"}";
    public static final String JSON_ARRAY_CONTENT = "{\"key\" :  1 , \"data\" :  \"tom\"}{\"key\" :  2 , \"data\" :  \"hog\"}";

    public static final String JSON_SCHEMA = "{ \"fields\": [{ \"name\": \"key\", \"type\": \"int\"} , { \"name\": \"data\", \"type\": \"string\"}]}";
    public static final String JSON_SCHEMA_INVAILD = "{ \"fields\": [{ \"error_name\": \"key\", \"type\": \"int\"} , { \"error_name\": \"data\", \"type\": \"string\"}]}";

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(ConvertJSONToORC.class);
    }

    @Test
    public void testBasicConversion() throws IOException {
        runner.setProperty(ConvertJSONToORC.SCHEMA, JSON_SCHEMA);
        runner.enqueue(streamFor(JSON_CONTENT, Charset.forName("utf8")));
        runner.run();
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship("success");
        assertNotNull(flowFile);
        assertEquals(1, flowFile.size());
        assertNotNull(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()));
        assertTrue(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()).endsWith(".orc"));
        assertEquals(1, Integer.parseInt(flowFile.get(0).getAttribute("record.count")));
    }

    @Test
    public void testBasicConversionForArray() throws IOException {
        runner.setProperty(ConvertJSONToORC.SCHEMA, JSON_SCHEMA);
        runner.enqueue(streamFor(JSON_ARRAY_CONTENT, Charset.forName("utf8")));
        runner.run();
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship("success");
        assertNotNull(flowFile);
        assertEquals(1, flowFile.size());
        assertNotNull(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()));
        assertTrue(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()).endsWith(".orc"));
        assertEquals(2, Integer.parseInt(flowFile.get(0).getAttribute("record.count")));
    }


    @Test
    public void testEmptyContent() throws IOException {
        runner.setProperty(ConvertJSONToORC.SCHEMA, JSON_SCHEMA);
        runner.enqueue(streamFor(JSON_EMPTY_CONTENT, Charset.forName("utf8")));
        runner.run();
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship("success");
        assertNotNull(flowFile);
        assertEquals(1, flowFile.size());
        assertNotNull(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()));
        assertTrue(flowFile.get(0).getAttribute(CoreAttributes.FILENAME.key()).endsWith(".orc"));
        assertEquals(0, Integer.parseInt(flowFile.get(0).getAttribute("record.count")));
    }


    @Test
    public void testInvalidSchema() throws IOException {
        ValidationContext vc = mock(ValidationContext.class);
        Validator val = ConvertJSONToORC.schema_validator();
        ValidationResult vr = val.validate("Json schema", JSON_SCHEMA_INVAILD, vc);
        assertFalse(vr.isValid());
    }

    @Test
    public void testFailureRelationship() throws IOException {
        runner.setProperty(ConvertJSONToORC.SCHEMA, JSON_SCHEMA);
        runner.enqueue(streamFor("{", Charset.forName("utf8")));
        runner.run();
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship("failure");
        assertNotNull(flowFile);
        assertEquals(1, flowFile.size());
    }


    public static InputStream streamFor(String content, Charset charset) throws CharacterCodingException {
        return new ByteArrayInputStream(bytesFor(content, charset));
    }

    public static byte[] bytesFor(String content, Charset charset) throws CharacterCodingException {
        CharBuffer chars = CharBuffer.wrap(content);
        CharsetEncoder encoder = charset.newEncoder();
        ByteBuffer buffer = encoder.encode(chars);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}