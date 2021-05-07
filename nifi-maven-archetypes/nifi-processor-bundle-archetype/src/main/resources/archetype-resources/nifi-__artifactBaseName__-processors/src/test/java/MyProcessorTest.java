package ${package};

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ${package}.MyProcessor.MY_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MyProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public void testProcessor() {
        // TODO: Replace with a test for your processor

        // Set the mandatory property
        testRunner.setProperty(MY_PROPERTY, "Something");

        // Add the content to the runner (just because we 'should' have some content).
        MockFlowFile flowfile = testRunner.enqueue("The content of the flowfile");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("Dummy", "1234567890");
        flowfile.putAttributes(attributes);


        // Run the enqueued content, it also takes an int = number of contents queued
        testRunner.run(1);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(MyProcessor.MY_RELATIONSHIP);
        assertEquals(1, results.size(), "Must be 1 match");
        MockFlowFile result = results.get(0);
        result.assertAttributeEquals("Reversed", "0987654321");
    }

}
