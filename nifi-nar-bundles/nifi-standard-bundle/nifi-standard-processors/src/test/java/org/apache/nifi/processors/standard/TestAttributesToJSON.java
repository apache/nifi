package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;


public class TestAttributesToJSON {

    private static Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.AttributesToJSON", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestAttributesToJSON", "debug");
        LOGGER = LoggerFactory.getLogger(TestAttributesToJSON.class);
    }

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

    @Test
    public void testInvalidJSONValueInAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());

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
    public void testAttribuets_emptyListUserSpecifiedAttributes() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());

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
    public void testAttribute_singleUserDefinedAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToJSON());
        testRunner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, TEST_ATTRIBUTE_KEY);

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
