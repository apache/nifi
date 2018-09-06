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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.Assert.fail;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final String HTTP_POST_METHOD = "POST";
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";

    private ListenHTTP proc;
    private TestRunner runner;

    private int availablePort;

    @Before
    public void setup() throws IOException {
        proc = new ListenHTTP();
        runner = TestRunners.newTestRunner(proc);
        availablePort = NetworkUtils.availablePort();
        runner.setVariable(PORT_VARIABLE, Integer.toString(availablePort));
        runner.setVariable(BASEPATH_VARIABLE, HTTP_BASE_PATH);

    }

    @After
    public void teardown() {
        proc.shutdownHttpServer();
    }

    @Test
    public void testPOSTRequestsReceivedWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);

        testPOSTRequestsReceived(HttpServletResponse.SC_OK);
    }

    @Test
    public void testPOSTRequestsReceivedReturnCodeWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT);
    }

    @Test
    public void testPOSTRequestsReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK);
    }

    @Test
    public void testPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService();
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService();
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService();
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService();
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT);
    }

    @Test
    public void testSecureInvalidSSLConfiguration() throws Exception {
        SSLContextService sslContextService = configureInvalidProcessorSslContextService();
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, "TLSv1.2");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertNotValid();
    }

    private int executePOST(String message) throws Exception {
        final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
        final boolean secure = (sslContextService != null);
        String endpointUrl = buildUrl(secure);
        final URL url = new URL(endpointUrl);
        HttpURLConnection connection;

        if (secure) {
            final HttpsURLConnection sslCon = (HttpsURLConnection) url.openConnection();
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.WANT);
            sslCon.setSSLSocketFactory(sslContext.getSocketFactory());
            connection = sslCon;

        } else {
            connection = (HttpURLConnection) url.openConnection();
        }
        connection.setRequestMethod(HTTP_POST_METHOD);
        connection.setDoOutput(true);

        final DataOutputStream wr = new DataOutputStream(connection.getOutputStream());

        if (message != null) {
            wr.writeBytes(message);
        }
        wr.flush();
        wr.close();
        return connection.getResponseCode();
    }

    private String buildUrl(final boolean secure) {
      return String.format("%s://localhost:%s/%s", secure ? "https" : "http" , availablePort,  HTTP_BASE_PATH);
    }

    private void testPOSTRequestsReceived(int returnCode) throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");
        messages.add("");
        messages.add(null);
        messages.add("payload 2");

        startWebServerAndSendMessages(messages, returnCode);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);
        mockFlowFiles.get(0).assertContentEquals("payload 1");
        mockFlowFiles.get(1).assertContentEquals("");
        mockFlowFiles.get(2).assertContentEquals("");
        mockFlowFiles.get(3).assertContentEquals("payload 2");
    }

    private void startWebServerAndSendRequests(Runnable sendRequestToWebserver, int numberOfExpectedFlowFiles, int returnCode) throws Exception {
      final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
      final ProcessContext context = runner.getProcessContext();
      proc.createHttpServer(context);

      new Thread(sendRequestToWebserver).start();

      long responseTimeout = 10000;

      int numTransferred = 0;
      long startTime = System.currentTimeMillis();
      while (numTransferred < numberOfExpectedFlowFiles && (System.currentTimeMillis() - startTime < responseTimeout)) {
          proc.onTrigger(context, processSessionFactory);
          numTransferred = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).size();
          Thread.sleep(100);
      }

      runner.assertTransferCount(ListenHTTP.RELATIONSHIP_SUCCESS, numberOfExpectedFlowFiles);
    }

    private void startWebServerAndSendMessages(final List<String> messages, int returnCode)
            throws Exception {

        Runnable sendMessagestoWebServer = () -> {
            try {
                for (final String message : messages) {
                    if (executePOST(message) != returnCode) {
                        fail("HTTP POST failed.");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Not expecting error here.");
            }
        };

        startWebServerAndSendRequests(sendMessagestoWebServer, messages.size(), returnCode);
    }

    private SSLContextService configureProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);
        return sslContextService;
    }

    private SSLContextService configureInvalidProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);
        return sslContextService;
    }


    @Test(/*timeout=10000*/)
    public void testMultipartFormDataRequest() throws Exception {

      runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
      runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
      runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));

      final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
      final boolean isSecure = (sslContextService != null);

      Runnable sendRequestToWebserver = () -> {
        try {
          MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM)
              .addFormDataPart("p1", "v1")
              .addFormDataPart("p2", "v2")
              .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(MediaType.parse("text/plain"), createTextFile("my-file-text.txt", "Hello", "World")))
              .addFormDataPart("file2", "my-file-data.json", RequestBody.create(MediaType.parse("application/json"), createTextFile("my-file-text.txt", "{ \"name\":\"John\", \"age\":30 }")))
              .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(MediaType.parse("application/octet-stream"), generateRandomBinaryData(100)))
              .build();

          Request request =
              new Request.Builder()
                .url(buildUrl(isSecure))
                .post(multipartBody)
                .build();

          int timeout = 3000;
          OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(timeout, TimeUnit.MILLISECONDS)
                .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                .build();

          try (Response response = client.newCall(request).execute()) {
            Assert.assertTrue(String.format("Unexpected code: %s, body: %s", response.code(), response.body().string()), response.isSuccessful());
          }
        } catch (final Throwable t) {
          t.printStackTrace();
          Assert.fail(t.toString());
        }
      };


      startWebServerAndSendRequests(sendRequestToWebserver, 5, 200);

      runner.assertAllFlowFilesTransferred(ListenHTTP.RELATIONSHIP_SUCCESS, 5);
      List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ListenHTTP.RELATIONSHIP_SUCCESS);
      // Part fragments are not processed in the order we submitted them.
      // We cannot rely on the order we sent them in.
      MockFlowFile mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p1");
      mff.assertAttributeEquals("http.multipart.name", "p1");
      mff.assertAttributeExists("http.multipart.size");
      mff.assertAttributeEquals("http.multipart.fragments.sequence.number", "1");
      mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
      mff.assertAttributeExists("http.headers.multipart.content-disposition");

      mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p2");
      mff.assertAttributeEquals("http.multipart.name", "p2");
      mff.assertAttributeExists("http.multipart.size");
      mff.assertAttributeExists("http.multipart.fragments.sequence.number");
      mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
      mff.assertAttributeExists("http.headers.multipart.content-disposition");

      mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file1");
      mff.assertAttributeEquals("http.multipart.name", "file1");
      mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
      mff.assertAttributeEquals("http.headers.multipart.content-type", "text/plain");
      mff.assertAttributeExists("http.multipart.size");
      mff.assertAttributeExists("http.multipart.fragments.sequence.number");
      mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
      mff.assertAttributeExists("http.headers.multipart.content-disposition");

      mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file2");
      mff.assertAttributeEquals("http.multipart.name", "file2");
      mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
      mff.assertAttributeEquals("http.headers.multipart.content-type", "application/json");
      mff.assertAttributeExists("http.multipart.size");
      mff.assertAttributeExists("http.multipart.fragments.sequence.number");
      mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
      mff.assertAttributeExists("http.headers.multipart.content-disposition");

      mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file3");
      mff.assertAttributeEquals("http.multipart.name", "file3");
      mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
      mff.assertAttributeEquals("http.headers.multipart.content-type", "application/octet-stream");
      mff.assertAttributeExists("http.multipart.size");
      mff.assertAttributeExists("http.multipart.fragments.sequence.number");
      mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
      mff.assertAttributeExists("http.headers.multipart.content-disposition");
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
}
