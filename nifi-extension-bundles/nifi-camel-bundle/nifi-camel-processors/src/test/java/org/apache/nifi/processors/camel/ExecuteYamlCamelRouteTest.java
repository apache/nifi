package org.apache.nifi.processors.camel;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ExecuteYamlCamelRouteTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(ExecuteCamelRoute.class);
    }

    @Test
    public void routeToOutSucceeds() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOut.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("key", "value");
            testRunner.enqueue("input1".getBytes(), attributes);
            testRunner.run(1);
            testRunner.assertTransferCount("out", 1);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 0);
            MockFlowFile f = testRunner.getFlowFilesForRelationship("out").getFirst();
            String actual = new String(testRunner.getContentAsByteArray(f)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex = "Received camel message: \"input1\" and headers \\{filename=.*?, key=value, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex, actual));
        }
    }

    @Test
    public void routeToOutSucceedsWithNewHeaders() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOutWithNewHeaders.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("key", "value");
            testRunner.enqueue("input1".getBytes(), attributes);
            testRunner.run(1);
            testRunner.assertTransferCount("out", 1);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 0);
            MockFlowFile f = testRunner.getFlowFilesForRelationship("out").getFirst();
            String actual = new String(testRunner.getContentAsByteArray(f)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex = "Received camel message: \"input1\" and headers \\{filename=.*?, key=value, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex, actual));
            Map<String, String> newAttributes = f.getAttributes();
            assert(newAttributes.containsKey("filename"));
            assert(newAttributes.get("filename").endsWith("-updated"));
            assert(newAttributes.containsKey("path"));
            assert(newAttributes.containsKey("uuid"));
            assert(newAttributes.containsKey("key"));
            assert(newAttributes.containsKey("newKey1"));
            assert(newAttributes.get("newKey1").equals("newValue1"));
            assert(newAttributes.containsKey("newKey2"));
            assert(newAttributes.get("newKey2").equals("3"));
            assert(newAttributes.containsKey("newKey3"));
            assert(newAttributes.get("newKey3").equals("true"));
        }
    }

    @Test
    public void routeToOutSucceedsWithMultipleFlowFiles() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOut.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);

            HashMap<String, String> attributes1 = new HashMap<>();
            attributes1.put("key1", "value1");
            testRunner.enqueue("input1".getBytes(), attributes1);

            HashMap<String, String> attributes2 = new HashMap<>();
            attributes2.put("key2", "value2");
            testRunner.enqueue("input2".getBytes(), attributes2);

            HashMap<String, String> attributes3 = new HashMap<>();
            attributes3.put("key3", "value3");
            testRunner.enqueue("input3".getBytes(), attributes3);

            testRunner.run(3);
            testRunner.assertTransferCount("out", 3);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 0);

            MockFlowFile f1 = testRunner.getFlowFilesForRelationship("out").getFirst();
            String actual1 = new String(testRunner.getContentAsByteArray(f1)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex1 = "Received camel message: \"input1\" and headers \\{filename=.*?, key1=value1, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex1, actual1));

            MockFlowFile f2 = testRunner.getFlowFilesForRelationship("out").get(1);
            String actual2 = new String(testRunner.getContentAsByteArray(f2)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex2 = "Received camel message: \"input2\" and headers \\{filename=.*?, key2=value2, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex2, actual2));

            MockFlowFile f3 = testRunner.getFlowFilesForRelationship("out").get(2);
            String actual3 = new String(testRunner.getContentAsByteArray(f3)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex3 = "Received camel message: \"input3\" and headers \\{filename=.*?, key3=value3, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex3, actual3));
        }
    }

    @Test
    public void routeToErrSucceeds() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToErr.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            testRunner.enqueue("input1".getBytes());
            testRunner.run(1);
            testRunner.assertTransferCount("out", 0);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 1);
            MockFlowFile f = testRunner.getFlowFilesForRelationship(ExecuteCamelRoute.REL_FAILURE).getFirst();
            String actual = new String(testRunner.getContentAsByteArray(f)).replaceAll("\r", "").replaceAll("\n", "");
            String expected = "input1";
            assert(actual.equals(expected));
            String errorMessage = f.getAttribute("camel.error.message");
            assert(errorMessage.startsWith("org.apache.nifi.processors.camel.NifiCamelDefaultErrRouteBuilder$NifiCamelRouteToErrException: Camel message routed to direct:err"));
        }
    }

    @Test
    public void routeFailsForBodyConversionMissing() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeWithoutBodyConversion.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            testRunner.enqueue("input1".getBytes());
            testRunner.run(1);
            testRunner.assertTransferCount("out", 0);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 1);
            MockFlowFile f = testRunner.getFlowFilesForRelationship(ExecuteCamelRoute.REL_FAILURE).getFirst();
            String actual = new String(testRunner.getContentAsByteArray(f)).replaceAll("\r", "").replaceAll("\n", "");
            String expected = "input1";
            assert(actual.equals(expected));
            String errorMessage = f.getAttribute("camel.error.message");
            assert(errorMessage.startsWith("java.lang.ClassCastException: Expected route output to be an instance of java.io.InputStream. Actual: java.lang.String"));
        }
    }

    @Test
    public void routeToOut1AndOut2Succeeds() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOut1AndOut2.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("key", "value");
            testRunner.enqueue("input1".getBytes(), attributes);
            testRunner.enqueue("input2".getBytes(), attributes);
            testRunner.run(2);
            testRunner.assertTransferCount("out1", 1);
            testRunner.assertTransferCount("out2", 1);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 0);

            MockFlowFile f1 = testRunner.getFlowFilesForRelationship("out1").getFirst();
            String actual1 = new String(testRunner.getContentAsByteArray(f1)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex1 = "Received camel message: \"input1\" and headers \\{filename=.*?, key=value, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex1, actual1));

            MockFlowFile f2 = testRunner.getFlowFilesForRelationship("out2").getFirst();
            String actual2 = new String(testRunner.getContentAsByteArray(f2)).replaceAll("\r", "").replaceAll("\n", "");
            String expectedRegex2 = "Received camel message: \"input2\" and headers \\{filename=.*?, key=value, path=.*?, uuid=.*}";
            assert(Pattern.matches(expectedRegex2, actual2));
        }
    }

    @Test
    public void routeWithoutEndpoint() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeWithoutEndpoint.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("key", "value");
            testRunner.enqueue("input3".getBytes(), attributes);
            testRunner.run(1);
            testRunner.assertTransferCount(ExecuteCamelRoute.REL_FAILURE, 1);

            MockFlowFile f = testRunner.getFlowFilesForRelationship(ExecuteCamelRoute.REL_FAILURE).getFirst();
            assert (f.getAttribute("camel.error.message").startsWith("java.lang.RuntimeException: No outgoing endpoint found in Camel exchange"));
        }
    }

    @Test
    public void routeSwapSucceeds() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOut.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            assert(testRunner.getProcessor().getRelationships().size() == 2);
            Set<Relationship> expected = new HashSet<>();
            expected.add(new Relationship.Builder().name("out").build());
            expected.add(new Relationship.Builder().name("failure").build());
            assert (testRunner.getProcessor().getRelationships().equals(expected));
        }
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("routeToOut1AndOut2.yaml")) {
            assert(is != null);
            String route = new String(is.readAllBytes());
            testRunner.setProperty(ExecuteCamelRoute.CAMEL_ROUTE, route);
            assert(testRunner.getProcessor().getRelationships().size() == 3);
            Set<Relationship> expected = new HashSet<>();
            expected.add(new Relationship.Builder().name("out1").build());
            expected.add(new Relationship.Builder().name("out2").build());
            expected.add(new Relationship.Builder().name("failure").build());
            assert (testRunner.getProcessor().getRelationships().equals(expected));
        }
    }

}
