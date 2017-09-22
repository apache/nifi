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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okio.Buffer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TestFetchADLSFile {

    private MockWebServer server;
    private ADLStoreClient client;
    private Processor processor;
    private TestRunner runner;
    Map<String, String> fileAttributes;

    private static final String respSimpleBinaryFileStatus = "{\"FileStatus\":{\"length\":120,\"pathSuffix\":\"\"," +
            "\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333747105,\"modificationTime\":1497333747246," +
            "\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0," +
            "\"aclBit\":false}}";
    private static final String respSimpleBinaryFileContent = "SEQ\u0006\u0019org.apache.hadoop.io.Text " +
            "org.apache.hadoop.io.IntWritable      ”ø\n" +
            "e*æ\u000Bà#Â÷\"7m#\u0017   \n" +
            "   \u0006\u0005Ricky   \u0016   \t   \u0005\u0004Jeff   $";
    private static final String respEmptyFileStatus = "{\"FileStatus\":{\"length\":0,\"pathSuffix\":\"\",\"type\":" +
            "\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333747105,\"modificationTime\":1497333747246," +
            "\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0," +
            "\"aclBit\":false}}";

    @Before
    public void init() throws IOException, InitializationException {
        server = new MockWebServer();
        QueueDispatcher dispatcher = new QueueDispatcher();
        dispatcher.setFailFast(new MockResponse().setResponseCode(400));
        server.setDispatcher(dispatcher);
        server.start();
        String accountFQDN = server.getHostName() + ":" + server.getPort();
        String dummyToken = "testDummyAadToken";

        client = ADLStoreClient.createClient(accountFQDN, dummyToken);
        client.setOptions(new ADLStoreOptions().setInsecureTransport());

        processor = new FetchADLSWithMockClient(client);
        runner = TestRunners.newTestRunner(processor);

        runner.setProperty(ADLSConstants.ACCOUNT_NAME, accountFQDN);
        runner.setProperty(ADLSConstants.CLIENT_ID, "foobar");
        runner.setProperty(ADLSConstants.CLIENT_SECRET, "foobar");
        runner.setProperty(ADLSConstants.AUTH_TOKEN_ENDPOINT, "foobar");
        runner.setProperty(FetchADLSFile.FILENAME, "${path}/${filename}");

        fileAttributes = new HashMap<>();
        fileAttributes.put("filename", "sample");
        fileAttributes.put("path", "/root/");
    }

    @Test
    public void testFetchInvalidEL() {
        final String filename = "src/test/resources/${literal('LoremIpsum.txt'):foo()}";
        runner.setProperty(FetchADLSFile.FILENAME, filename);
        runner.assertNotValid();
    }

    @Test
    public void testFetchBinaryBasic() {

        server.enqueue(getMockResponse(respSimpleBinaryFileStatus));
        server.enqueue(getMockResponse(respSimpleBinaryFileContent));
        runner.enqueue(new byte[]{}, fileAttributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
        long fileSize = runner.getFlowFilesForRelationship(ADLSConstants.REL_SUCCESS).get(0).getSize();
        Assert.assertEquals(120, fileSize);
    }

    @Test
    public void testFetchEmptyFile() {

        server.enqueue(getMockResponse(respEmptyFileStatus));
        runner.enqueue(new byte[]{}, fileAttributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchFailure() {

        server.enqueue(new MockResponse().setResponseCode(400).setBody(respEmptyFileStatus));
        server.enqueue(new MockResponse().setResponseCode(404).setBody(respEmptyFileStatus));
        server.enqueue(new MockResponse().setResponseCode(500).setBody(respEmptyFileStatus));
        runner.enqueue(new byte[]{}, fileAttributes);
        runner.enqueue(new byte[]{}, fileAttributes);
        runner.enqueue(new byte[]{}, fileAttributes);
        runner.run(3);
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 3);
    }

    @Test
    public void testFetchSkipsWithoutFlowFile() {

        server.enqueue(getMockResponse("OK"));
        runner.run(1);
        runner.assertTransferCount(ADLSConstants.REL_SUCCESS, 0);
        runner.assertTransferCount(ADLSConstants.REL_FAILURE, 0);
    }


    private MockResponse getMockResponse(String body) {
        return (new MockResponse())
                .setResponseCode(200)
                .setBody(body);
    }

    private MockResponse getMockResponse(Buffer buffer) {
        return (new MockResponse())
                .setResponseCode(200)
                .setBody(buffer);
    }

    public class FetchADLSWithMockClient extends FetchADLSFile {
        FetchADLSWithMockClient(ADLStoreClient client) {
            this.adlStoreClient = client;
        }
    }

}
