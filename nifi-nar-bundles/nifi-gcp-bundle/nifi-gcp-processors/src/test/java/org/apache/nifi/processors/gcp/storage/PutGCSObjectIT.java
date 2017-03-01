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

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import java.util.Map;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_ALGORITHM_ATTR;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link PutGCSObject} which actually use Google Cloud resources.
 */
public class PutGCSObjectIT extends AbstractGCSIT {
    private static final String KEY = "delete-me";
    private static final byte[] CONTENT = {12, 13, 14};

    @Test
    public void testSimplePut() throws Exception {
        final TestRunner runner = buildNewRunner(new PutGCSObject());
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
        runner.setProperty(PutGCSObject.KEY, KEY);

        runner.enqueue(CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 1);
        assertTrue(fileEquals(KEY, CONTENT));

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS).get(0);
        flowFile.assertAttributeNotExists(ENCRYPTION_ALGORITHM_ATTR);

        for (Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    @Test
    public void testEncryptedPut() throws Exception {
        final TestRunner runner = buildNewRunner(new PutGCSObject());
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
        runner.setProperty(PutGCSObject.KEY, KEY);
        runner.setProperty(PutGCSObject.ENCRYPTION_KEY, ENCRYPTION_KEY);

        runner.enqueue(CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 1);
        assertTrue(fileEqualsEncrypted(KEY, CONTENT));

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListGCSBucket.REL_SUCCESS).get(0);
        flowFile.assertAttributeExists(ENCRYPTION_ALGORITHM_ATTR);

        for (Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    @Test
    public void testPutWithAcl() throws Exception {
        final TestRunner runner = buildNewRunner(new PutGCSObject());
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
        runner.setProperty(PutGCSObject.KEY, KEY);
        runner.setProperty(PutGCSObject.ACL, PutGCSObject.ACL_BUCKET_OWNER_READ);

        runner.enqueue(CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 1);
        assertTrue(fileEquals(KEY, CONTENT));

        final Blob blob = storage.get(BlobId.of(BUCKET, KEY));

        boolean userIsOwner = false;
        boolean projectOwnerIsReader = false;
        for (Acl acl : blob.listAcls()) {
            if (acl.getEntity().getType() == Acl.Entity.Type.USER
                    && acl.getRole() == Acl.Role.OWNER) {
                userIsOwner = true;
            }

            if (acl.getEntity().getType() == Acl.Entity.Type.PROJECT
                    && acl.getRole() == Acl.Role.READER) {
                projectOwnerIsReader = true;
            }
        }

        assertTrue(userIsOwner);
        assertTrue(projectOwnerIsReader);
    }

    @Test
    public void testPutWithOverwrite() throws Exception {
        final TestRunner runner = buildNewRunner(new PutGCSObject());
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
        runner.setProperty(PutGCSObject.KEY, KEY);

        putTestFile(KEY, new byte[]{1, 2});

        runner.enqueue(CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_SUCCESS, 1);
        assertTrue(fileEquals(KEY, CONTENT));

    }


    @Test
    public void testPutWithNoOverwrite() throws Exception {
        final TestRunner runner = buildNewRunner(new PutGCSObject());
        runner.setProperty(PutGCSObject.BUCKET, BUCKET);
        runner.setProperty(PutGCSObject.KEY, KEY);
        runner.setProperty(PutGCSObject.OVERWRITE, "false");

        putTestFile(KEY, new byte[]{1, 2});

        runner.enqueue(CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListGCSBucket.REL_FAILURE, 1);
    }
}
