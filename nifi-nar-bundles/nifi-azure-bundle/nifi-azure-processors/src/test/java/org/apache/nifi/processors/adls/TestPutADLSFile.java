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
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class TestPutADLSFile {

    private MockWebServer server;
    private ADLStoreClient client;
    private Processor processor;
    private TestRunner runner;
    Map<String, String> fileAttributes;

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

        processor = new PutADLSWithMockClient(client);

        runner = TestRunners.newTestRunner(processor);

        runner.setProperty(ADLSConstants.ACCOUNT_NAME, accountFQDN);
        runner.setProperty(ADLSConstants.CLIENT_ID, "foobar");
        runner.setProperty(ADLSConstants.CLIENT_SECRET, "foobar");
        runner.setProperty(ADLSConstants.AUTH_TOKEN_ENDPOINT, "foobar");
        runner.setProperty(PutADLSFile.DIRECTORY, "/sample/");
        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.FAIL_RESOLUTION_AV);
        runner.removeProperty(PutADLSFile.UMASK);
        runner.removeProperty(PutADLSFile.ACL);

        fileAttributes = new HashMap<>();
        fileAttributes.put("filename", "sample.txt");
        fileAttributes.put("path", "/root/");
    }

    @Test
    public void testPutConflictFail() throws IOException, InterruptedException {

        //for create call
        server.enqueue(new MockResponse().setResponseCode(200));
        //for append call(internal call while writing to stream)
        server.enqueue(new MockResponse().setResponseCode(200));
        //for rename call(since its fail conflict)
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":true}"));
        //for delete call(removing temp file)
        server.enqueue(new MockResponse().setResponseCode(200));

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpCreateRequest = createRequest.getRequestUrl().queryParameter("op");
        String pathCreateRequest = createRequest.getPath();
        String queryOverwiteCreateRequest = createRequest.getRequestUrl().queryParameter("overwrite");
        Assert.assertEquals("CREATE", queryOpCreateRequest);
        Assert.assertEquals("true", queryOverwiteCreateRequest);
        String expectedPath = "sample.txt.nifipart";
        Assert.assertThat(pathCreateRequest, CoreMatchers.containsString(expectedPath));

        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathAppendRequest = appendRequest.getPath();
        String bodyAppendRequest = appendRequest.getBody().toString();
        String queryOpAppendRequest = appendRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("APPEND", queryOpAppendRequest);
        Assert.assertEquals("[text="+bodySimpleFileContent+"]", bodyAppendRequest);
        Assert.assertThat(pathAppendRequest, CoreMatchers.containsString(expectedPath));

        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathRenameRequest = renameRequest.getPath();
        String queryOpRenameRequest = renameRequest.getRequestUrl().queryParameter("op");
        String queryOverwriteRenameRequest = renameRequest.getRequestUrl().queryParameter("overwrite");
        Assert.assertThat(pathRenameRequest, CoreMatchers.containsString(expectedPath));
        Assert.assertEquals("RENAME", queryOpRenameRequest);
        Assert.assertEquals(null, queryOverwriteRenameRequest);

        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathDeleteRequest = deleteRequest.getPath();
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);
        Assert.assertThat(pathDeleteRequest, CoreMatchers.containsString(expectedPath));

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutConflictReplace() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.REPLACE_RESOLUTION_AV);

        //for create call
        server.enqueue(new MockResponse().setResponseCode(200));
        //for append call(internal call while writing to stream)
        server.enqueue(new MockResponse().setResponseCode(200));
        //for rename call(since its fail conflict)
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":true}"));
        //for delete call(removing temp file)
        server.enqueue(new MockResponse().setResponseCode(200));

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpCreateRequest = createRequest.getRequestUrl().queryParameter("op");
        String pathCreateRequest = createRequest.getPath();
        String queryOverwiteCreateRequest = createRequest.getRequestUrl().queryParameter("overwrite");
        Assert.assertEquals("CREATE", queryOpCreateRequest);
        Assert.assertEquals("true", queryOverwiteCreateRequest);
        String expectedPath = "sample.txt.nifipart";
        Assert.assertThat(pathCreateRequest, CoreMatchers.containsString(expectedPath));

        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathAppendRequest = appendRequest.getPath();
        String bodyAppendRequest = appendRequest.getBody().toString();
        String queryOpAppendRequest = appendRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("APPEND", queryOpAppendRequest);
        Assert.assertEquals("[text="+bodySimpleFileContent+"]", bodyAppendRequest);
        Assert.assertThat(pathAppendRequest, CoreMatchers.containsString(expectedPath));

        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathRenameRequest = renameRequest.getPath();
        String queryOpRenameRequest = renameRequest.getRequestUrl().queryParameter("op");
        String queryOverwriteRenameRequest = renameRequest.getRequestUrl().queryParameter("renameoptions");
        Assert.assertThat(pathRenameRequest, CoreMatchers.containsString(expectedPath));
        Assert.assertEquals("RENAME", queryOpRenameRequest);
        Assert.assertEquals("OVERWRITE", queryOverwriteRenameRequest.toUpperCase());

        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathDeleteRequest = deleteRequest.getPath();
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);
        Assert.assertThat(pathDeleteRequest, CoreMatchers.containsString(expectedPath));

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutConflictAppend() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.APPEND_RESOLUTION_AV);

        //for create call
        server.enqueue(new MockResponse().setResponseCode(200));
        //for append call(internal call while writing to stream)
        server.enqueue(new MockResponse().setResponseCode(200));
        //for rename call(since its fail conflict)
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":true}"));
        //for delete call(removing temp file)
        server.enqueue(new MockResponse().setResponseCode(200));

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();
        String tempFilePath = "sample.txt.nifipart";
        String originalFilePath = "sample.txt";

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpCreateRequest = createRequest.getRequestUrl().queryParameter("op");
        String pathCreateRequest = createRequest.getPath();
        String queryOverwiteCreateRequest = createRequest.getRequestUrl().queryParameter("overwrite");
        Assert.assertEquals("CREATE", queryOpCreateRequest);
        Assert.assertEquals("true", queryOverwiteCreateRequest);
        Assert.assertThat(pathCreateRequest, CoreMatchers.containsString(tempFilePath));

        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathAppendRequest = appendRequest.getPath();
        String bodyAppendRequest = appendRequest.getBody().toString();
        String queryOpAppendRequest = appendRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("APPEND", queryOpAppendRequest);
        Assert.assertEquals("[text="+bodySimpleFileContent+"]", bodyAppendRequest);
        Assert.assertThat(pathAppendRequest, CoreMatchers.containsString(tempFilePath));

        RecordedRequest concatenateRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathConcatRequest = concatenateRequest.getPath();
        String queryOpConcatRequest = concatenateRequest.getRequestUrl().queryParameter("op");
        String bodyConcatRequest = concatenateRequest.getBody().toString();
        Assert.assertThat(pathConcatRequest, CoreMatchers.containsString(originalFilePath));
        Assert.assertEquals("MSCONCAT", queryOpConcatRequest);
        Assert.assertThat(bodyConcatRequest, CoreMatchers.containsString(tempFilePath));

        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String pathDeleteRequest = deleteRequest.getPath();
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);
        Assert.assertThat(pathDeleteRequest, CoreMatchers.containsString(tempFilePath));

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutDirectoryEmpty() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.DIRECTORY, "");

        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutDirectoryRoot() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.DIRECTORY, "\\");

        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutDirectoryEL() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.DIRECTORY, "${path}");

        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);

        String bodySimpleFileContent = "sample content to be send to external adls resource";
        runner.enqueue(bodySimpleFileContent, fileAttributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    @Test
    public void testPutUMask() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.UMASK, "577");
        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);
        runner.enqueue("sample data", fileAttributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        Assert.assertEquals("577", recordedRequest.getRequestUrl().queryParameter("permission"));
    }

    @Test
    public void testPutUMaskInvalid() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.UMASK, "999");
        runner.assertNotValid();
    }


    @Test
    public void testPutACL() throws IOException, InterruptedException {

        String sampleACLEntry = "default:user:bob:r-x, default:user:bob:r-x";
        String expectedACLEntry = "default:user:bob:r-x,default:user:bob:r-x";
        runner.setProperty(PutADLSFile.ACL, sampleACLEntry);
        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        runner.enqueue("sample data", fileAttributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        recordedRequest = server.takeRequest(1, TimeUnit.SECONDS);
        Assert.assertEquals(expectedACLEntry, recordedRequest.getRequestUrl().queryParameter("aclspec"));
    }

    @Test
    public void testPutACLInvalid() throws IOException, InterruptedException {

        String sampleACLEntry = "default:user:bob:r-x, default:r-x";
        String expectedACLEntry = "default:user:bob:r-x,default:user:bob:r-x";
        runner.setProperty(PutADLSFile.ACL, sampleACLEntry);
        MockResponse mockResponse = new MockResponse().setResponseCode(200);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        server.enqueue(mockResponse.setBody("{\"boolean\":true}"));
        server.enqueue(mockResponse);
        server.enqueue(mockResponse);
        runner.enqueue("sample data", fileAttributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }


    @Test
    public void testPutInvalidCreate() throws IOException, InterruptedException {

        server.enqueue(new MockResponse().setResponseCode(400));
        server.enqueue(new MockResponse().setResponseCode(404));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutInvalidWrite() throws IOException, InterruptedException {

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(403));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutInvalidConflictFail() throws IOException, InterruptedException {

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(403));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutUnsuccesfullConflictFail() throws IOException, InterruptedException {

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":false}"));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutInvalidConflictReplace() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.REPLACE_RESOLUTION_AV);
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(403));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutUnsuccesfullConflictReplace() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.REPLACE_RESOLUTION_AV);
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":false}"));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutInvalidAppend() throws IOException, InterruptedException {

        runner.setProperty(PutADLSFile.CONFLICT_RESOLUTION, PutADLSFile.APPEND_RESOLUTION_AV);
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(400));
        server.enqueue(new MockResponse().setResponseCode(200));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_FAILURE, 1);
    }

    @Test
    public void testPutInvalidDelete() throws IOException, InterruptedException {

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"boolean\":true}"));
        server.enqueue(new MockResponse().setResponseCode(400));
        runner.enqueue("sample data", fileAttributes);
        runner.run();

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest appendRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest renameRequest = server.takeRequest(1, TimeUnit.SECONDS);
        RecordedRequest deleteRequest = server.takeRequest(1, TimeUnit.SECONDS);
        String queryOpDeleteRequest = deleteRequest.getRequestUrl().queryParameter("op");
        Assert.assertEquals("DELETE", queryOpDeleteRequest);

        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 1);
    }

    public class PutADLSWithMockClient extends PutADLSFile {
        PutADLSWithMockClient(ADLStoreClient client) {
            this.adlStoreClient = client;
        }
    }
}
