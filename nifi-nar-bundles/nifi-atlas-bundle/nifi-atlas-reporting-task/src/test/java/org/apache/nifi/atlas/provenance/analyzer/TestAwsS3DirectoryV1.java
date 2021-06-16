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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.junit.Test;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.ATTR_BUCKET_V1;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.ATTR_OBJECT_PREFIX_V1;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.TYPE_BUCKET_V1;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.TYPE_DIRECTORY_V1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestAwsS3DirectoryV1 extends AbstractTestAwsS3Directory {

    @Override
    protected String getAwsS3ModelVersion() {
        return "v1";
    }

    @Test
    public void testSimpleDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testCompoundDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1/dir2/dir3/dir4/dir5";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testRootDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testWithPutORC() {
        String processorName = "PutORC";
        String dirPath = "/dir1";

        executeTest(processorName, dirPath);
    }

    @Override
    protected void assertAnalysisResult(DataSetRefs refs, String dirPath) {
        String expectedDirectoryQualifiedName = String.format("s3a://%s%s@%s", AWS_BUCKET, dirPath, ATLAS_NAMESPACE);
        String expectedBucketQualifiedName = String.format("s3a://%s@%s", AWS_BUCKET, ATLAS_NAMESPACE);

        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());

        Referenceable directoryRef = refs.getOutputs().iterator().next();

        assertEquals(TYPE_DIRECTORY_V1, directoryRef.getTypeName());
        assertEquals(expectedDirectoryQualifiedName, directoryRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(dirPath, directoryRef.get(ATTR_NAME));
        assertEquals(dirPath, directoryRef.get(ATTR_OBJECT_PREFIX_V1));

        Referenceable bucketRef = (Referenceable) directoryRef.get(ATTR_BUCKET_V1);
        assertNotNull(bucketRef);
        assertEquals(TYPE_BUCKET_V1, bucketRef.getTypeName());
        assertEquals(expectedBucketQualifiedName, bucketRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(AWS_BUCKET, bucketRef.get(ATTR_NAME));
    }
}
