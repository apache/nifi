package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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
        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeNotExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

        testRunner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).get(0).
                assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
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

}
