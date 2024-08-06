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
package org.apache.nifi.processors.gcp.storage;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link FetchGCSObject} which actually use Google Cloud resources.
 */
public class FetchGCSObjectIT extends AbstractGCSIT {
    static final String KEY = "delete-me";
    static final byte[] CONTENT = {10, 11, 12};
    private static final Logger logger = LoggerFactory.getLogger(FetchGCSObjectIT.class);

    @Test
    public void testSimpleFetch() throws Exception {
        putTestFile(KEY, CONTENT);
        assertTrue(fileExists(KEY));

        final TestRunner runner = buildNewRunner(new FetchGCSObject());
        runner.setProperty(FetchGCSObject.BUCKET, BUCKET);

        runner.enqueue(new byte[0], Collections.singletonMap(
                "filename", KEY
        ));

        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);

        ff.assertContentEquals(CONTENT);

        ff.assertAttributeNotExists(StorageAttributes.ENCRYPTION_ALGORITHM_ATTR);
        ff.assertAttributeNotExists(StorageAttributes.ENCRYPTION_SHA256_ATTR);

        for (final Map.Entry<String, String> entry : ff.getAttributes().entrySet()) {
            logger.info("{}:{}", entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testSimpleFetchEncrypted() throws Exception {
        putTestFileEncrypted(KEY, CONTENT);
        assertTrue(fileExists(KEY));

        final FetchGCSObject processor = new FetchGCSObject();
        final TestRunner runner = buildNewRunner(processor);
        runner.setProperty(FetchGCSObject.BUCKET, BUCKET);
        runner.setProperty(FetchGCSObject.ENCRYPTION_KEY, ENCRYPTION_KEY);

        runner.enqueue(new byte[0], Collections.singletonMap(
                "filename", KEY
        ));

        runner.assertValid();
        runner.run();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", KEY);
        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), attributes);
        assertEquals(2, results.size());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().matches("Successfully fetched \\[delete-me\\] from Bucket \\[gcloud-test-bucket-temp-.*\\], totaling 3 bytes"));

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchGCSObject.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);

        ff.assertAttributeEquals(StorageAttributes.ENCRYPTION_ALGORITHM_ATTR,
                "AES256");
    }


    @Test
    public void testFetchNonexistantFile() throws Exception {
        final TestRunner runner = buildNewRunner(new FetchGCSObject());
        runner.setProperty(FetchGCSObject.BUCKET, BUCKET);
        runner.enqueue(new byte[0], Collections.singletonMap(
                "filename", "non-existent"
        ));

        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchGCSObject.REL_FAILURE, 1);
    }
}
