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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DeleteGCSObject}. No connections to the Google Cloud service are made.
 */
public class DeleteGCSObjectTest extends AbstractGCSTest {
    public static final Long GENERATION = 42L;
    static final String KEY = "somefile";


    public static final String BUCKET_ATTR = "gcs.bucket";
    public static final String KEY_ATTR = "gcs.key";
    public static final String GENERATION_ATTR = "gcs.generation";

    @Mock
    Storage storage;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(DeleteGCSObject.BUCKET, BUCKET);
        runner.setProperty(DeleteGCSObject.GENERATION, String.valueOf(GENERATION));
        runner.setProperty(DeleteGCSObject.KEY, KEY);
    }

    @Override
    public DeleteGCSObject getProcessor() {
        return new DeleteGCSObject() {
            @Override
            protected Storage getCloudService() {
                return storage;
            }
        };
    }


    @Test
    public void testDeleteWithValidArguments() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        runner.assertValid();

        runner.enqueue("testdata");

        runner.run();

        verify(storage).delete(eq(BlobId.of(BUCKET, KEY, GENERATION)));

        runner.assertAllFlowFilesTransferred(DeleteGCSObject.REL_SUCCESS);
        runner.assertTransferCount(DeleteGCSObject.REL_SUCCESS, 1);
    }

    @Test
    public void testTwoDeletesWithFlowfileAttributes() throws Exception {
        reset(storage);

        final TestRunner runner = buildNewRunner(getProcessor());

        runner.setProperty(DeleteGCSObject.BUCKET, "${" + BUCKET_ATTR + "}");
        runner.setProperty(DeleteGCSObject.KEY, "${" + KEY_ATTR + "}");
        runner.setProperty(DeleteGCSObject.GENERATION, "${" + GENERATION_ATTR + "}");

        runner.assertValid();

        final String bucket1 = BUCKET + "_1";
        final String bucket2 = BUCKET + "_2";
        final String key1 = KEY + "_1";
        final String key2 = KEY + "_2";
        final Long generation1 = GENERATION + 1L;
        final Long generation2 = GENERATION + 2L;

        runner.enqueue("testdata1", ImmutableMap.of(
                BUCKET_ATTR, bucket1,
                KEY_ATTR, key1,
                GENERATION_ATTR, String.valueOf(generation1)
        ));

        runner.enqueue("testdata2", ImmutableMap.of(
                BUCKET_ATTR, bucket2,
                KEY_ATTR, key2,
                GENERATION_ATTR, String.valueOf(generation2)
        ));

        runner.run(2);

        verify(storage).delete(eq(BlobId.of(bucket1, key1, generation1)));
        verify(storage).delete(eq(BlobId.of(bucket2, key2, generation2)));

        runner.assertAllFlowFilesTransferred(DeleteGCSObject.REL_SUCCESS);
        runner.assertTransferCount(DeleteGCSObject.REL_SUCCESS, 2);
    }

    @Test
    public void testFailureOnException() throws Exception {
        reset(storage);
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        runner.enqueue("testdata");

        when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(1, "Test Exception"));

        runner.run();

        runner.assertPenalizeCount(1);
        runner.assertAllFlowFilesTransferred(DeleteGCSObject.REL_FAILURE);
        runner.assertTransferCount(DeleteGCSObject.REL_FAILURE, 1);
    }
}
