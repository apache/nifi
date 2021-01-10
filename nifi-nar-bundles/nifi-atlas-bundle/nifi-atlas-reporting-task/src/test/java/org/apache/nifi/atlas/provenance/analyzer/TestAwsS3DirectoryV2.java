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

import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.junit.Test;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.ATTR_CONTAINER_V2;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.ATTR_OBJECT_PREFIX_V2;
import static org.apache.nifi.atlas.provenance.analyzer.AwsS3Directory.TYPE_BUCKET_V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestAwsS3DirectoryV2 extends AbstractTestAwsS3Directory {

    @Override
    protected String getAwsS3ModelVersion() {
        return AtlasPathExtractorUtil.AWS_S3_ATLAS_MODEL_VERSION_V2;
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

    protected void assertAnalysisResult(DataSetRefs refs, String dirPath) {
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());

        Referenceable ref = refs.getOutputs().iterator().next();

        String actualPath = dirPath;
        while (StringUtils.isNotEmpty(actualPath) && !"/".equals(actualPath)) {
            String directory = StringUtils.substringAfterLast(actualPath, "/");

            assertEquals(AwsS3Directory.TYPE_DIRECTORY_V2, ref.getTypeName());
            assertEquals(String.format("s3a://%s%s/@%s", AWS_BUCKET, actualPath, ATLAS_NAMESPACE), ref.get(ATTR_QUALIFIED_NAME));
            assertEquals(directory, ref.get(ATTR_NAME));
            assertEquals(actualPath + "/", ref.get(ATTR_OBJECT_PREFIX_V2));
            assertNotNull(ref.get(ATTR_CONTAINER_V2));

            ref = (Referenceable) ref.get(ATTR_CONTAINER_V2);
            actualPath = StringUtils.substringBeforeLast(actualPath, "/");
        }

        assertEquals(TYPE_BUCKET_V2, ref.getTypeName());
        assertEquals(String.format("s3a://%s@%s", AWS_BUCKET, ATLAS_NAMESPACE), ref.get(ATTR_QUALIFIED_NAME));
        assertEquals(AWS_BUCKET, ref.get(ATTR_NAME));
    }
}
