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
package org.apache.nifi.processors.aws.s3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestDeleteS3Object {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    // When you want to test this, you should create a bucket on Amazon S3 as follows.
    private final String TEST_REGION = "us-east-1";
    private final String TEST_BUCKET = "test-bucket-00000000-0000-0000-0000-1234567890123";

    @Before
    public void setUp() throws IOException, URISyntaxException {
        final TestRunner runner = TestRunners.newTestRunner(new PutS3Object());
        runner.setProperty(PutS3Object.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutS3Object.BUCKET, TEST_BUCKET);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "hello.txt");
        URL file = this.getClass().getClassLoader().getResource("hello.txt");
        runner.enqueue(Paths.get(file.toURI()), attrs);
        runner.run(1);
    }

    @Test
    public void testSimpleDelete() throws IOException {
        DeleteS3Object deleter = new DeleteS3Object();
        final TestRunner runner = TestRunners.newTestRunner(deleter);
        runner.setProperty(DeleteS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(DeleteS3Object.REGION, TEST_REGION);
        runner.setProperty(DeleteS3Object.BUCKET, TEST_BUCKET);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "hello.txt");
        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }
}
