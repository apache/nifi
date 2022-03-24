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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ITestHandleHttpRequest {

    private HandleHttpRequest processor;

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    @BeforeClass
    public static void configureServices() throws TlsException  {
        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        if (processor != null) {
            processor.shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testRequestAddedToService() throws InitializationException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();
                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    final HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(30000);
                    connection.setReadTimeout(30000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);
        assertEquals(1, contextMap.size());

        final MockFlowFile mff = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("http.query.param.query", "true");
        mff.assertAttributeEquals("http.query.param.value1", "value1");
        mff.assertAttributeEquals("http.query.param.value2", "");
        mff.assertAttributeEquals("http.query.param.value3", "");
        mff.assertAttributeEquals("http.query.param.value4", "apple=orange");
        mff.assertAttributeEquals("http.headers.header1", "value1");
        mff.assertAttributeEquals("http.headers.header3", "apple=orange");
    }

    @Test(timeout = 30000)
    public void testMultipartFormDataRequest() throws InitializationException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

                    MultipartBody multipartBody = new MultipartBody.Builder()
                            .setType(MultipartBody.FORM)
                            .addFormDataPart("p1", "v1")
                            .addFormDataPart("p2", "v2")
                            .addFormDataPart("file1", "my-file-text.txt",
                                    RequestBody.create(createTextFile("Hello", "World"), MediaType.parse("text/plain")))
                            .addFormDataPart("file2", "my-file-data.json",
                                    RequestBody.create(createTextFile( "{ \"name\":\"John\", \"age\":30 }"), MediaType.parse("application/json")))
                            .addFormDataPart("file3", "my-file-binary.bin",
                                    RequestBody.create(generateRandomBinaryData(), MediaType.parse("application/octet-stream")))
                            .build();

                    Request request = new Request.Builder()
                            .url(String.format("http://localhost:%s/my/path", port))
                            .post(multipartBody).build();

                    OkHttpClient client =
                            new OkHttpClient.Builder()
                                    .readTimeout(3000, TimeUnit.MILLISECONDS)
                                    .writeTimeout(3000, TimeUnit.MILLISECONDS)
                                    .build();

                    sendRequest(client, request, requestSent);
                } catch (Exception e) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 5);
        assertEquals(1, contextMap.size());

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS);

        // Part fragments are not processed in the order we submitted them.
        // We cannot rely on the order we sent them in.
        MockFlowFile mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p1");
        String contextId = mff.getAttribute(HTTPUtils.HTTP_CONTEXT_ID);
        mff.assertAttributeEquals("http.multipart.name", "p1");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeEquals("http.multipart.fragments.sequence.number", "1");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");


        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p2");
        // each part generates a corresponding flow file - yet all parts are coming from the same request,
        mff.assertAttributeEquals(HTTPUtils.HTTP_CONTEXT_ID, contextId);
        mff.assertAttributeEquals("http.multipart.name", "p2");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");


        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file1");
        mff.assertAttributeEquals(HTTPUtils.HTTP_CONTEXT_ID, contextId);
        mff.assertAttributeEquals("http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "text/plain");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");


        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file2");
        mff.assertAttributeEquals(HTTPUtils.HTTP_CONTEXT_ID, contextId);
        mff.assertAttributeEquals("http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/json");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");


        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file3");
        mff.assertAttributeEquals(HTTPUtils.HTTP_CONTEXT_ID, contextId);
        mff.assertAttributeEquals("http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/octet-stream");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");
    }

    @Test(timeout = 30000)
    public void testMultipartFormDataRequestCaptureFormAttributes() throws InitializationException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");
        runner.setProperty(HandleHttpRequest.PARAMETERS_TO_ATTRIBUTES, "p1,p2");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

                    MultipartBody multipartBody = new MultipartBody.Builder()
                        .setType(MultipartBody.FORM)
                        .addFormDataPart("p1", "v1")
                        .addFormDataPart("p2", "v2")
                        .addFormDataPart("p3", "v3")
                        .build();

                    Request request = new Request.Builder()
                        .url(String.format("http://localhost:%s/my/path", port))
                        .post(multipartBody).build();

                    OkHttpClient client =
                        new OkHttpClient.Builder()
                            .readTimeout(3000, TimeUnit.MILLISECONDS)
                            .writeTimeout(3000, TimeUnit.MILLISECONDS)
                            .build();

                    sendRequest(client, request, requestSent);
                } catch (Exception e) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 3);
        assertEquals(1, contextMap.size());

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS);

        // Part fragments are not processed in the order we submitted them.
        // We cannot rely on the order we sent them in.
        for (int i = 1; i < 4; i++) {
            MockFlowFile mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", String.format("p%d", i));
            mff.assertAttributeEquals("http.multipart.name", String.format("p%d", i));
            mff.assertAttributeExists("http.param.p1");
            mff.assertAttributeEquals("http.param.p1", "v1");
            mff.assertAttributeExists("http.param.p2");
            mff.assertAttributeEquals("http.param.p2", "v2");
            mff.assertAttributeNotExists("http.param.p3");
        }
    }

    @Test(timeout = 30000)
    public void testMultipartFormDataRequestFailToRegisterContext() throws InitializationException, InterruptedException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);
        CountDownLatch resultReady = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        contextMap.setRegisterSuccessfully(false);
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        AtomicInteger responseCode = new AtomicInteger(0);
        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

                    MultipartBody multipartBody = new MultipartBody.Builder()
                            .setType(MultipartBody.FORM)
                            .addFormDataPart("p1", "v1")
                            .addFormDataPart("p2", "v2")
                            .addFormDataPart("file1", "my-file-text.txt",
                                    RequestBody.create(createTextFile("my-file-text.txt", "Hello", "World"), MediaType.parse("text/plain")))
                            .addFormDataPart("file2", "my-file-data.json",
                                    RequestBody.create(createTextFile("my-file-text.txt", "{ \"name\":\"John\", \"age\":30 }"), MediaType.parse("application/json")))
                            .addFormDataPart("file3", "my-file-binary.bin",
                                    RequestBody.create(generateRandomBinaryData(), MediaType.parse("application/octet-stream")))
                            .build();

                    Request request = new Request.Builder()
                            .url(String.format("http://localhost:%s/my/path", port))
                            .post(multipartBody).build();

                    OkHttpClient client =
                            new OkHttpClient.Builder()
                                    .readTimeout(20000, TimeUnit.MILLISECONDS)
                                    .writeTimeout(20000, TimeUnit.MILLISECONDS)
                                    .build();

                    Callback callback = new Callback() {
                        @Override
                        public void onFailure(@NotNull Call call, @NotNull IOException e) {
                            // Not going to happen
                        }

                        @Override
                        public void onResponse(@NotNull Call call, @NotNull Response response) {
                            responseCode.set(response.code());
                            resultReady.countDown();
                        }
                    };
                    sendRequest(client, request, callback, requestSent);
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);
        resultReady.await();

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 0);
        assertEquals(0, contextMap.size());
        Assert.assertEquals(503, responseCode.get());
    }

    private byte[] generateRandomBinaryData() {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        return bytes;
    }


    private File createTextFile(String... lines) throws IOException {
        File file = new File(getClass().getSimpleName());
        file.deleteOnExit();
        try (final PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            for (final String line : lines) {
                writer.println(line);
            }
        }
        return file;
    }


    protected MockFlowFile findFlowFile(List<MockFlowFile> flowFilesForRelationship, String attributeName, String attributeValue) {
        Optional<MockFlowFile> optional = flowFilesForRelationship.stream().filter(ff -> ff.getAttribute(attributeName).equals(attributeValue)).findFirst();
        Assert.assertTrue(optional.isPresent());
        return optional.get();
    }


    @Test(timeout = 30000)
    public void testFailToRegister() throws InitializationException, InterruptedException {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);
        CountDownLatch resultReady = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");
        contextMap.setRegisterSuccessfully(false);

        final int[] responseCode = new int[1];
        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                HttpURLConnection connection = null;
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    connection = (HttpURLConnection) new URL("http://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();
                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(20000);
                    connection.setReadTimeout(20000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    if (connection != null) {
                        try {
                            responseCode[0] = connection.getResponseCode();
                        } catch (IOException e) {
                            responseCode[0] = -1;
                        }
                    } else {
                        responseCode[0] = -2;
                    }
                } finally {
                    resultReady.countDown();
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);
        resultReady.await();

        runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
        assertEquals(503, responseCode[0]);
    }

    @Test
    public void testCleanup() throws Exception {
        // GIVEN
        int nrOfRequests = 5;

        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(nrOfRequests);
        CountDownLatch cleanupDone = new CountDownLatch(nrOfRequests - 1);

        processor = new HandleHttpRequest() {
            @Override
            synchronized void initializeServer(ProcessContext context) throws Exception {
                super.initializeServer(context);
                serverReady.countDown();

                requestSent.await();
                while (getRequestQueueSize() < nrOfRequests) {
                    Thread.sleep(200);
                }
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        List<Response> responses = new ArrayList<>(nrOfRequests);
        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

                    OkHttpClient client =
                            new OkHttpClient.Builder()
                                    .readTimeout(3000, TimeUnit.MILLISECONDS)
                                    .writeTimeout(3000, TimeUnit.MILLISECONDS)
                                    .build();
                    client.dispatcher().setMaxRequests(nrOfRequests);
                    client.dispatcher().setMaxRequestsPerHost(nrOfRequests);

                    Callback callback = new Callback() {
                        @Override
                        public void onFailure(@NotNull Call call, @NotNull IOException e) {
                            // Will only happen once for the first non-rejected request, but not important
                        }

                        @Override
                        public void onResponse(@NotNull Call call, @NotNull Response response) {
                            responses.add(response);
                            cleanupDone.countDown();
                        }
                    };
                    IntStream.rangeClosed(1, nrOfRequests).forEach(
                            requestCounter -> {
                                Request request = new Request.Builder()
                                        .url(String.format("http://localhost:%s/my/" + requestCounter, port))
                                        .get()
                                        .build();
                                sendRequest(client, request, callback, requestSent);
                            }
                    );
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        // WHEN
        httpThread.start();
        runner.run(1, false);
        cleanupDone.await();

        // THEN
        int nrOfPendingRequests = processor.getRequestQueueSize();

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);

        assertEquals(1, contextMap.size());
        assertEquals(0, nrOfPendingRequests);
        assertEquals(responses.size(), nrOfRequests - 1);
        for (Response response : responses) {
            assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE, response.code());
        }
    }

    @Test(timeout = 15000)
    public void testOnPrimaryNodeChangePrimaryNodeRevoked() throws Exception {
        processor = new HandleHttpRequest();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final int port = NetworkUtils.getAvailableTcpPort();
        runner.setProperty(HandleHttpRequest.PORT, Integer.toString(port));

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        final String contextMapId = MockHttpContextMap.class.getSimpleName();
        runner.addControllerService(contextMapId, contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, contextMapId);

        final ProcessContext processContext = spy(runner.getProcessContext());
        when(processContext.getExecutionNode()).thenReturn(ExecutionNode.PRIMARY);
        processor.initializeServer(processContext);

        final OkHttpClient client = new OkHttpClient.Builder().build();

        final String url = String.format("http://localhost:%d", port);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final CountDownLatch requestCompleted = new CountDownLatch(1);
        final CountDownLatch requestStarted = new CountDownLatch(1);

        final AtomicReference<IOException> requestException = new AtomicReference<>();
        final AtomicInteger responseStatus = new AtomicInteger();
        executorService.execute(() -> {
            final Request request = new Request.Builder().url(url).get().build();
            final Call call = client.newCall(request);
            call.enqueue(new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    requestException.set(e);
                    requestCompleted.countDown();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) {
                    responseStatus.set(response.code());
                    requestCompleted.countDown();
                }
            });
            requestStarted.countDown();
        });

        requestStarted.await();
        Thread.sleep(1000);
        processor.onPrimaryNodeChange(PrimaryNodeState.PRIMARY_NODE_REVOKED);
        requestCompleted.await();

        assertNull("HTTP Request Exception found", requestException.get());
        assertEquals("HTTP Status not matched", HttpServletResponse.SC_SERVICE_UNAVAILABLE, responseStatus.get());
    }

    @Test
    public void testSecure() throws Exception {
        secureTest(false);
    }

    @Test
    public void testSecureTwoWaySsl() throws Exception {
        secureTest(true);
    }

    private void secureTest(boolean twoWaySsl) throws Exception {
        CountDownLatch serverReady = new CountDownLatch(1);
        CountDownLatch requestSent = new CountDownLatch(1);

        processor = createProcessor(serverReady, requestSent);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final RestrictedSSLContextService sslContextService = mock(RestrictedSSLContextService.class);
        final String serviceIdentifier = RestrictedSSLContextService.class.getName();
        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        Mockito.when(sslContextService.createContext()).thenReturn(keyStoreSslContext);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(HandleHttpRequest.SSL_CONTEXT, serviceIdentifier);

        final Thread httpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.await();

                    final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                    final HttpsURLConnection connection = (HttpsURLConnection) new URL("https://localhost:"
                            + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                    SSLContext clientSslContext;
                    if (twoWaySsl) {
                        // Use a client certificate, do not reuse the server's keystore
                        clientSslContext = keyStoreSslContext;
                    } else {
                        // With one-way SSL, the client still needs a truststore
                        clientSslContext = trustStoreSslContext;
                    }
                    connection.setSSLSocketFactory(clientSslContext.getSocketFactory());
                    connection.setDoOutput(false);
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("header1", "value1");
                    connection.setRequestProperty("header2", "");
                    connection.setRequestProperty("header3", "apple=orange");
                    connection.setConnectTimeout(3000);
                    connection.setReadTimeout(3000);

                    sendRequest(connection, requestSent);
                } catch (final Throwable t) {
                    // Do nothing as HandleHttpRequest doesn't respond normally
                }
            }
        });

        httpThread.start();
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 1);
        assertEquals(1, contextMap.size());

        final MockFlowFile mff = runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("http.query.param.query", "true");
        mff.assertAttributeEquals("http.query.param.value1", "value1");
        mff.assertAttributeEquals("http.query.param.value2", "");
        mff.assertAttributeEquals("http.query.param.value3", "");
        mff.assertAttributeEquals("http.query.param.value4", "apple=orange");
        mff.assertAttributeEquals("http.headers.header1", "value1");
        mff.assertAttributeEquals("http.headers.header3", "apple=orange");
        mff.assertAttributeEquals("http.protocol", "HTTP/1.1");
    }

    private HandleHttpRequest createProcessor(CountDownLatch serverReady, CountDownLatch requestSent) {
        return new HandleHttpRequest() {
            @Override
            synchronized void initializeServer(ProcessContext context) throws Exception {
                super.initializeServer(context);
                serverReady.countDown();

                requestSent.await();
                while (getRequestQueueSize() == 0) {
                    Thread.sleep(200);
                }
            }

            @Override
            void drainContainerQueue() {
                // Skip this, otherwise it would wait to make sure there are no more requests
            }
        };
    }

    private void sendRequest(HttpURLConnection connection, CountDownLatch requestSent) throws Exception {
        Future<InputStream> executionFuture = Executors.newSingleThreadExecutor()
                .submit(connection::getInputStream);

        requestSent.countDown();

        executionFuture.get();
    }

    private void sendRequest(OkHttpClient client, Request request, CountDownLatch requestSent) {
        Callback callback = new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                // We (may) get a timeout as the processor doesn't answer unless there is some kind of error
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) {
                // Not called as the processor doesn't answer unless there is some kind of error
            }
        };

        sendRequest(client, request, callback, requestSent);
    }

    private void sendRequest(OkHttpClient client, Request request, Callback callback, CountDownLatch requestSent) {
        client.newCall(request).enqueue(callback);
        requestSent.countDown();
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private boolean registerSuccessfully = true;

        private final ConcurrentMap<String, HttpServletResponse> responseMap = new ConcurrentHashMap<>();

        @Override
        public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
            if (registerSuccessfully) {
                responseMap.put(identifier, response);
            }
            return registerSuccessfully;
        }

        @Override
        public HttpServletResponse getResponse(final String identifier) {
            return responseMap.get(identifier);
        }

        @Override
        public void complete(final String identifier) {
            responseMap.remove(identifier);
        }

        public int size() {
            return responseMap.size();
        }

        public void setRegisterSuccessfully(boolean registerSuccessfully) {
            this.registerSuccessfully = registerSuccessfully;
        }

        @Override
        public long getRequestTimeout(TimeUnit timeUnit) {
            return timeUnit.convert(30000, TimeUnit.MILLISECONDS);
        }
    }
}
