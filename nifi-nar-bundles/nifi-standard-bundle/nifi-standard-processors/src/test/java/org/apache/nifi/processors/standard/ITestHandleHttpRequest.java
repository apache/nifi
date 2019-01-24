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

import com.google.api.client.util.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ITestHandleHttpRequest {

    private static Map<String, String> getTruststoreProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/truststore.jks");
        props.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "passwordpassword");
        props.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return props;
    }

    private static Map<String, String> getKeystoreProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/keystore.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "passwordpassword");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        return properties;
    }

    private static SSLContext useSSLContextService(final TestRunner controller, final Map<String, String> sslProperties) {
        final SSLContextService service = new StandardRestrictedSSLContextService();
        try {
            controller.addControllerService("ssl-service", service, sslProperties);
            controller.enableControllerService(service);
        } catch (InitializationException ex) {
            ex.printStackTrace();
            Assert.fail("Could not create SSL Context Service");
        }

        controller.setProperty(HandleHttpRequest.SSL_CONTEXT, "ssl-service");
        return service.createSSLContext(SSLContextService.ClientAuth.WANT);
    }

    @Test(timeout=30000)
    public void testRequestAddedToService() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                        final HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:"
                                + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();
                        connection.setDoOutput(false);
                        connection.setRequestMethod("GET");
                        connection.setRequestProperty("header1", "value1");
                        connection.setRequestProperty("header2", "");
                        connection.setRequestProperty("header3", "apple=orange");
                        connection.setConnectTimeout(3000);
                        connection.setReadTimeout(3000);

                        StreamUtils.copy(connection.getInputStream(), new NullOutputStream());
                    } catch (final Throwable t) {
                        t.printStackTrace();
                        Assert.fail(t.toString());
                    }
                }
            });
            httpThread.start();

            while ( runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).isEmpty() ) {
                // process the request.
                runner.run(1, false, false);
            }

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
        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }


    @Test(timeout=30000)
    public void testMultipartFormDataRequest() throws InitializationException, MalformedURLException, IOException, InterruptedException {
      final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
      runner.setProperty(HandleHttpRequest.PORT, "0");

      final MockHttpContextMap contextMap = new MockHttpContextMap();
      runner.addControllerService("http-context-map", contextMap);
      runner.enableControllerService(contextMap);
      runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

      // trigger processor to stop but not shutdown.
      runner.run(1, false);
      try {
        final Thread httpThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {

              final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

              MultipartBody multipartBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("p1", "v1")
                .addFormDataPart("p2", "v2")
                .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(MediaType.parse("text/plain"), createTextFile("my-file-text.txt", "Hello", "World")))
                .addFormDataPart("file2", "my-file-data.json", RequestBody.create(MediaType.parse("application/json"), createTextFile("my-file-text.txt", "{ \"name\":\"John\", \"age\":30 }")))
                .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(MediaType.parse("application/octet-stream"), generateRandomBinaryData(100)))
                .build();

              Request request = new Request.Builder()
              .url(String.format("http://localhost:%s/my/path", port))
              .post(multipartBody).build();

              OkHttpClient client =
                  new OkHttpClient.Builder()
                    .readTimeout(3000, TimeUnit.MILLISECONDS)
                    .writeTimeout(3000, TimeUnit.MILLISECONDS)
                  .build();

              try (Response response = client.newCall(request).execute()) {
                Assert.assertTrue(String.format("Unexpected code: %s, body: %s", response.code(), response.body().string()), response.isSuccessful());
              }
            } catch (final Throwable t) {
              t.printStackTrace();
              Assert.fail(t.toString());
            }
          }
        });
        httpThread.start();

        while ( runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).isEmpty() ) {
          // process the request.
          runner.run(1, false, false);
        }

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
      } finally {
        // shut down the server
        runner.run(1, true);
      }
    }

    @Test(timeout=30000)
    public void testMultipartFormDataRequestFailToRegisterContext() throws InitializationException, MalformedURLException, IOException, InterruptedException {
      final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
      runner.setProperty(HandleHttpRequest.PORT, "0");

      final MockHttpContextMap contextMap = new MockHttpContextMap();
      contextMap.setRegisterSuccessfully(false);
      runner.addControllerService("http-context-map", contextMap);
      runner.enableControllerService(contextMap);
      runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

      // trigger processor to stop but not shutdown.
      runner.run(1, false);
      try {
        AtomicInteger responseCode = new AtomicInteger(0);
        final Thread httpThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {

              final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();

              MultipartBody multipartBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("p1", "v1")
                .addFormDataPart("p2", "v2")
                .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(MediaType.parse("text/plain"), createTextFile("my-file-text.txt", "Hello", "World")))
                .addFormDataPart("file2", "my-file-data.json", RequestBody.create(MediaType.parse("application/json"), createTextFile("my-file-text.txt", "{ \"name\":\"John\", \"age\":30 }")))
                .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(MediaType.parse("application/octet-stream"), generateRandomBinaryData(100)))
                .build();

              Request request = new Request.Builder()
              .url(String.format("http://localhost:%s/my/path", port))
              .post(multipartBody).build();

              OkHttpClient client =
                  new OkHttpClient.Builder()
                    .readTimeout(20000, TimeUnit.MILLISECONDS)
                    .writeTimeout(20000, TimeUnit.MILLISECONDS)
                  .build();

              try (Response response = client.newCall(request).execute()) {
                responseCode.set(response.code());
              }
            } catch (final Throwable t) {
              t.printStackTrace();
              Assert.fail(t.toString());
            }
          }
        });
        httpThread.start();

        while (responseCode.get() == 0) {
          // process the request.
          runner.run(1, false, false);
        }

        runner.assertAllFlowFilesTransferred(HandleHttpRequest.REL_SUCCESS, 0);
        assertEquals(0, contextMap.size());
        Assert.assertEquals(503, responseCode.get());
      } finally {
        // shut down the server
        runner.run(1, true);
      }
    }

    private byte[] generateRandomBinaryData(int i) {
      byte[] bytes = new byte[100];
      new Random().nextBytes(bytes);
      return bytes;
    }


    private File createTextFile(String fileName, String... lines) throws IOException {
      File file = new File(fileName);
      file.deleteOnExit();
      for (String string : lines) {
        Files.append(string, file, Charsets.UTF_8);
      }
      return file;
    }


    protected MockFlowFile findFlowFile(List<MockFlowFile> flowFilesForRelationship, String attributeName, String attributeValue) {
      Optional<MockFlowFile> optional = Iterables.tryFind(flowFilesForRelationship, ff -> ff.getAttribute(attributeName).equals(attributeValue));
      Assert.assertTrue(optional.isPresent());
      return optional.get();
    }


    @Test(timeout=30000)
    public void testFailToRegister() throws InitializationException, MalformedURLException, IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");
        contextMap.setRegisterSuccessfully(false);

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final int[] responseCode = new int[1];
            responseCode[0] = 0;
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    HttpURLConnection connection = null;
                    try {
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

                        StreamUtils.copy(connection.getInputStream(), new NullOutputStream());
                    } catch (final Throwable t) {
                        t.printStackTrace();
                        if(connection != null ) {
                            try {
                                responseCode[0] = connection.getResponseCode();
                            } catch (IOException e) {
                                responseCode[0] = -1;
                            }
                        } else {
                            responseCode[0] = -2;
                        }
                    }
                }
            });
            httpThread.start();

            while (responseCode[0] == 0) {
                // process the request.
                runner.run(1, false, false);
            }

            runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
            assertEquals(503, responseCode[0]);

        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }

    @Test
    public void testSecure() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(HandleHttpRequest.class);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        final MockHttpContextMap contextMap = new MockHttpContextMap();
        runner.addControllerService("http-context-map", contextMap);
        runner.enableControllerService(contextMap);
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, "http-context-map");

        final Map<String, String> sslProperties = getKeystoreProperties();
        sslProperties.putAll(getTruststoreProperties());
        sslProperties.put(StandardSSLContextService.SSL_ALGORITHM.getName(), "TLSv1.2");
        final SSLContext sslContext = useSSLContextService(runner, sslProperties);

        // trigger processor to stop but not shutdown.
        runner.run(1, false);
        try {
            final Thread httpThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final int port = ((HandleHttpRequest) runner.getProcessor()).getPort();
                        final HttpsURLConnection connection = (HttpsURLConnection) new URL("https://localhost:"
                                + port + "/my/path?query=true&value1=value1&value2=&value3&value4=apple=orange").openConnection();

                        connection.setSSLSocketFactory(sslContext.getSocketFactory());
                        connection.setDoOutput(false);
                        connection.setRequestMethod("GET");
                        connection.setRequestProperty("header1", "value1");
                        connection.setRequestProperty("header2", "");
                        connection.setRequestProperty("header3", "apple=orange");
                        connection.setConnectTimeout(3000);
                        connection.setReadTimeout(3000);

                        StreamUtils.copy(connection.getInputStream(), new NullOutputStream());
                    } catch (final Throwable t) {
                        t.printStackTrace();
                        Assert.fail(t.toString());
                    }
                }
            });
            httpThread.start();

            while ( runner.getFlowFilesForRelationship(HandleHttpRequest.REL_SUCCESS).isEmpty() ) {
                // process the request.
                runner.run(1, false, false);
            }

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
        } finally {
            // shut down the server
            runner.run(1, true);
        }
    }

    private static class MockHttpContextMap extends AbstractControllerService implements HttpContextMap {

        private boolean registerSuccessfully = true;

        private final ConcurrentMap<String, HttpServletResponse> responseMap = new ConcurrentHashMap<>();

        @Override
        public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
            if(registerSuccessfully) {
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

        public boolean isRegisterSuccessfully() {
            return registerSuccessfully;
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
