package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestHoldFile {
	private TestRunner testRunner;

	@Before
	public void init() {
		testRunner = TestRunners.newTestRunner(HoldFile.class);
	}

	@Test
	public void testHoldAndRelease() throws IOException {
		testRunner.setProperty(HoldFile.MAX_SIGNAL_AGE, "24 hours");
		testRunner.setProperty(HoldFile.RELEASE_SIGNAL_ATTRIBUTE, "identifier");
		testRunner.setProperty(HoldFile.FAILURE_ATTRIBUTE, "service.failed");
		testRunner.setProperty(HoldFile.COPY_SIGNAL_ATTRIBUTES, "true");
		
		// One signal file
		Map<String, String> signal = new HashMap<String, String>();
		signal.put(HoldFile.FLOW_FILE_RELEASE_VALUE, "1234567");
		signal.put("return.value", "a response");
		testRunner.enqueue("signal".getBytes(), signal);

		// Two normal flow files to be held
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("identifier", "123456");

		Map<String, String> metadata2 = new HashMap<String, String>();
		metadata2.put("identifier", "1234567");
		
		testRunner.enqueue("file1".getBytes(), metadata);
		testRunner.enqueue("file2".getBytes(), metadata2);

		testRunner.run();
		
		// One was held because it didn't match
		testRunner.assertTransferCount(HoldFile.REL_HOLD, 1);
		// The matching one was released
		testRunner.assertTransferCount(HoldFile. REL_RELEASE, 1);

		// Make sure the propagated attribute from signal was added to the held flow file
		MockFlowFile released = testRunner
				.getFlowFilesForRelationship(HoldFile.REL_RELEASE).get(0);
		assertEquals("Signal attributes were not copied to held file", 
				"a response", released.getAttribute("return.value"));
		assertNull("flow.file.release.value should not be propagated", released
				.getAttribute(HoldFile.FLOW_FILE_RELEASE_VALUE));

		// None expired
		testRunner.assertTransferCount(HoldFile.REL_EXPIRED, 0);

		// None failed
		testRunner.assertTransferCount(HoldFile.REL_FAILURE, 0);
	}

	@Test
	public void testHoldAndRelease_noCopy() throws IOException {
		testRunner.setProperty(HoldFile.MAX_SIGNAL_AGE, "24 hours");
		testRunner.setProperty(HoldFile.RELEASE_SIGNAL_ATTRIBUTE, "identifier");
		testRunner.setProperty(HoldFile.FAILURE_ATTRIBUTE, "service.failed");
		testRunner.setProperty(HoldFile.COPY_SIGNAL_ATTRIBUTES, "false");
		
		// One signal file
		Map<String, String> signal = new HashMap<String, String>();
		signal.put(HoldFile.FLOW_FILE_RELEASE_VALUE, "1234567");
		signal.put("return.value", "a response");
		testRunner.enqueue("signal".getBytes(), signal);

		// Two normal flow files to be held
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("identifier", "123456");

		Map<String, String> metadata2 = new HashMap<String, String>();
		metadata2.put("identifier", "1234567");
		
		testRunner.enqueue("file1".getBytes(), metadata);
		testRunner.enqueue("file2".getBytes(), metadata2);

		testRunner.run();
		
		// One was held because it didn't match
		testRunner.assertTransferCount(HoldFile.REL_HOLD, 1);
		// The matching one was released
		testRunner.assertTransferCount(HoldFile. REL_RELEASE, 1);

		// Make sure the propagated attribute from signal was added to the held flow file
		MockFlowFile released = testRunner
				.getFlowFilesForRelationship(HoldFile.REL_RELEASE).get(0);
		assertNull("Attributes were not supposed to be copied", released
				.getAttribute("return.value"));
		assertNull("flow.file.release.value should not be propagated", released
				.getAttribute(HoldFile.FLOW_FILE_RELEASE_VALUE));

		// None expired
		testRunner.assertTransferCount(HoldFile.REL_EXPIRED, 0);

		// None failed
		testRunner.assertTransferCount(HoldFile.REL_FAILURE, 0);
	}
	
	@Test
	public void testHoldAndRelease_failure() throws IOException {
		testRunner.setProperty(HoldFile.MAX_SIGNAL_AGE, "24 hours");
		testRunner.setProperty(HoldFile.RELEASE_SIGNAL_ATTRIBUTE, "identifier");
		testRunner.setProperty(HoldFile.FAILURE_ATTRIBUTE, "service.failed");
		testRunner.setProperty(HoldFile.COPY_SIGNAL_ATTRIBUTES, "false");
		
		// One signal file
		Map<String, String> signal = new HashMap<String, String>();
		signal.put(HoldFile.FLOW_FILE_RELEASE_VALUE, "1234567");
		signal.put("return.value", "a response");
		signal.put("service.failed", "true");
		testRunner.enqueue("signal".getBytes(), signal);

		// Two normal flow files to be held
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("identifier", "123456");

		Map<String, String> metadata2 = new HashMap<String, String>();
		metadata2.put("identifier", "1234567");
		
		testRunner.enqueue("file1".getBytes(), metadata);
		testRunner.enqueue("file2".getBytes(), metadata2);

		testRunner.run();
		
		// One was held because it didn't match
		testRunner.assertTransferCount(HoldFile.REL_HOLD, 1);
		// The matching one was routed to failure because of the
		// service.failed attribute
		testRunner.assertTransferCount(HoldFile. REL_FAILURE, 1);

		// Make sure the propagated attribute from signal was added to the held flow file
		MockFlowFile released = testRunner
				.getFlowFilesForRelationship(HoldFile.REL_FAILURE).get(0);
		assertNull("Attributes were not supposed to be copied", released
				.getAttribute("return.value"));

		// None expired
		testRunner.assertTransferCount(HoldFile.REL_EXPIRED, 0);
	}
}
