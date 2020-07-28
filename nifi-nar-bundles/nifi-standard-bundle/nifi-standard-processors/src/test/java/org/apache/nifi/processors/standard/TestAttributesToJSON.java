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

package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestAttributesToJSON {

    private static final String TEST_ATTRIBUTE_KEY = "TestAttribute";
    private static final String TEST_ATTRIBUTE_VALUE = "TestValue";

    @Test(expected = AssertionError.class)
    public void testInvalidUserSuppliedAttributeList() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());

        //Attribute list CANNOT be empty
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIncludeCoreAttributesProperty() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "val1,val2");
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "maybe");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();
    }

    @Test
    public void testNullValueForEmptyAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToJSON.NULL_VALUE_FOR_EMPTY_STRING, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non existing attribute
        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        assertNull(val.get(NON_PRESENT_ATTRIBUTE_KEY));
    }

    @Test
    public void testEmptyStringValueForEmptyAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToJSON.NULL_VALUE_FOR_EMPTY_STRING, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non existing attribute
        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        assertEquals(val.get(NON_PRESENT_ATTRIBUTE_KEY), "");
    }

    @Test
    public void testInvalidJSONValueInAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        //Create attribute that contains an invalid JSON Character
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, "'badjson'");

        testRunner.enqueue(ff);
        testRunner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
    }


    @Test
    public void testAttributes_emptyListUserSpecifiedAttributes() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
    }


    @Test
    public void testContent_emptyListUserSpecifiedAttributes() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeNotExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).assertContentEquals("{}");
    }

    @Test
    public void testAttribute_singleUserDefinedAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, TEST_ATTRIBUTE_KEY);
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
        assertTrue(val.size() == 1);
    }


    @Test
    public void testAttribute_singleUserDefinedAttributeWithWhiteSpace() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, " " + TEST_ATTRIBUTE_KEY + " ");
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertTrue(val.get(TEST_ATTRIBUTE_KEY).equals(TEST_ATTRIBUTE_VALUE));
        assertTrue(val.size() == 1);
    }


    @Test
    public void testAttribute_singleNonExistingUserDefinedAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "NonExistingAttribute");
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);

        //If a Attribute is requested but does not exist then it is placed in the JSON with an empty string
        assertTrue(val.get("NonExistingAttribute").equals(""));
        assertTrue(val.size() == 1);
    }

    @Test
    public void testAttribute_noIncludeCoreAttributesUserDefined() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, " " + TEST_ATTRIBUTE_KEY + " , " + CoreAttributes.PATH.key() + " ");
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);
        ff = session.putAttribute(ff, CoreAttributes.PATH.key(), TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0)
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .get(0).getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(json, HashMap.class);
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(1, val.size());
    }

    @Test
    public void testAttribute_noIncludeCoreAttributesContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);
        ff = session.putAttribute(ff, CoreAttributes.PATH.key(), TEST_ATTRIBUTE_VALUE);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> val = mapper.readValue(testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).toByteArray(), HashMap.class);
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(1, val.size());
    }

    @Test
    public void testAttribute_includeCoreAttributesContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertEquals(AttributesToJSON.APPLICATION_JSON, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        Map<String, String> val = new ObjectMapper().readValue(flowFile.toByteArray(), HashMap.class);
        assertEquals(3, val.size());
        Set<String> coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toSet());
        val.keySet().forEach(k -> assertTrue(coreAttributes.contains(k)));
    }

    @Test
    public void testAttribute_includeCoreAttributesAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        Map<String, String> val = new ObjectMapper().readValue(flowFile.getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME), HashMap.class);
        assertEquals(3, val.size());
        Set<String> coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toSet());
        val.keySet().forEach(k -> assertTrue(coreAttributes.contains(k)));
    }

    @Test
    public void testAttributesRegex() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setVariable("regex", "delimited\\.header\\.column\\.[0-9]+");
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_REGEX, "${regex}");
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "test, test1");

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("delimited.header.column.1", "Registry");
        attributes.put("delimited.header.column.2", "Assignment");
        attributes.put("delimited.header.column.3", "Organization Name");
        attributes.put("delimited.header.column.4", "Organization Address");
        attributes.put("delimited.footer.column.1", "not included");
        attributes.put("test", "test");
        attributes.put("test1", "test1");
        testRunner.enqueue("".getBytes(), attributes);

        testRunner.run();

        testRunner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0);

        Map<String, String> val = new ObjectMapper().readValue(flowFile.getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME), HashMap.class);
        assertTrue(val.keySet().contains("delimited.header.column.1"));
        assertTrue(val.keySet().contains("delimited.header.column.2"));
        assertTrue(val.keySet().contains("delimited.header.column.3"));
        assertTrue(val.keySet().contains("delimited.header.column.4"));
        assertTrue(!val.keySet().contains("delimited.footer.column.1"));
        assertTrue(val.keySet().contains("test"));
        assertTrue(val.keySet().contains("test1"));
    }
}
