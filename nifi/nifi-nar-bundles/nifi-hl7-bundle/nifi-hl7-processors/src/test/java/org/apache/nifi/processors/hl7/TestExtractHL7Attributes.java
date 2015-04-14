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
package org.apache.nifi.processors.hl7;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.nifi.processors.hl7.ExtractHL7Attributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestExtractHL7Attributes {

	@Test
	public void testExtract() throws IOException {
		System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
		final TestRunner runner = TestRunners.newTestRunner(ExtractHL7Attributes.class);
		runner.enqueue(Paths.get("src/test/resources/hypoglycemia.hl7"));
		
		runner.run();
		runner.assertAllFlowFilesTransferred(ExtractHL7Attributes.REL_SUCCESS, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(ExtractHL7Attributes.REL_SUCCESS).get(0);
		final SortedMap<String, String> sortedAttrs = new TreeMap<>(out.getAttributes());
		for (final Map.Entry<String, String> entry : sortedAttrs.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}
	
}
