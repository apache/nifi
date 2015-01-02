package org.apache.nifi.processors.kafka;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;


@Ignore("Intended only for local testing to verify functionality.")
public class TestPutKafka {

	@Test
	public void testKeyValuePut() {
		final TestRunner runner = TestRunners.newTestRunner(PutKafka.class);
		runner.setProperty(PutKafka.SEED_BROKERS, "192.168.0.101:9092");
		runner.setProperty(PutKafka.TOPIC, "${kafka.topic}");
		runner.setProperty(PutKafka.KEY, "${kafka.key}");
		runner.setProperty(PutKafka.TIMEOUT, "3 secs");
		runner.setProperty(PutKafka.DELIVERY_GUARANTEE, PutKafka.DELIVERY_REPLICATED.getValue());
		
		final Map<String, String> attributes = new HashMap<>();
		attributes.put("kafka.topic", "test");
		attributes.put("kafka.key", "key3");
		
		final byte[] data = "Hello, World, Again! ;)".getBytes();
		runner.enqueue(data, attributes);
		runner.enqueue(data, attributes);
		runner.enqueue(data, attributes);
		runner.enqueue(data, attributes);
		
		runner.run(5);
		
		runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 4);
		final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS);
		final MockFlowFile mff = mffs.get(0);
		
		assertTrue(Arrays.equals(data, mff.toByteArray()));
	}
	
}
