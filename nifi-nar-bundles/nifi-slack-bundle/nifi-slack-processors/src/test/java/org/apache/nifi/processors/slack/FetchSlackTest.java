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
package org.apache.nifi.processors.slack;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.Before;
import org.junit.Test;

public class FetchSlackTest {

  private TestRunner runner;

  @Before
  public void setUp() {
    runner = TestRunners.newTestRunner(FetchSlack.class);
    runner.setProperty(FetchSlack.API_TOKEN, "test-token");
  }

  @Test
  public void testEmptyBody() {
    runner.enqueue("");
    runner.run(2);

    runner.assertAllFlowFilesTransferred(FetchSlack.REL_FAILURE, 1);
    runner.assertQueueEmpty();
  }

  @Test
  public void testInvalidBody() {
    runner.enqueue("invalid");
    runner.run(2);

    runner.assertAllFlowFilesTransferred(FetchSlack.REL_FAILURE, 1);
    runner.assertQueueEmpty();
  }

  @Test
  public void testNoAttachement() {
    runner.enqueue("{\"type\":\"hello\"}");
    runner.run(2);

    runner.assertAllFlowFilesTransferred(FetchSlack.REL_FAILURE, 1);
    runner.assertQueueEmpty();
  }

  @Test
  public void testMultipleAttachementsCreateMultipleFlowFiles() throws Exception {
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(TestContentServlet.class, "/*");

    TestServer testServer = new TestServer();
    testServer.addHandler(handler);
    try {
      testServer.startServer();
      String url = testServer.getUrl();


      String data = "{" +
        "  \"type\": \"message\"," +
        "  \"files\": [" +
        "    {" +
        "      \"id\": \"test_id\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name.png\"," +
        "      \"title\": \"test_title\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/test_name.png\"" +
        "    }," +
        "    {" +
        "      \"id\": \"test_id2\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name2.png\"," +
        "      \"title\": \"test_title2\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/test_name2.png\"" +
        "    }" +
        "  ]" +
        "}";
      runner.enqueue(data);
      runner.run(2);

      runner.assertQueueEmpty();
      runner.assertTransferCount(FetchSlack.REL_RESPONSE, 2);
      runner.assertTransferCount(FetchSlack.REL_SUCCESS_REQ, 1);
      runner.assertTransferCount(FetchSlack.REL_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_NO_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_FAILURE, 0);

    } finally {
      testServer.shutdownServer();
    }
  }

  @Test
  public void testMultipleAttachementsWithFailureNoRetry() throws Exception {
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(TestContentServlet.class, "/*");

    TestServer testServer = new TestServer();
    testServer.addHandler(handler);
    try {
      testServer.startServer();
      String url = testServer.getUrl();


      String data = "{" +
        "  \"type\": \"message\"," +
        "  \"files\": [" +
        "    {" +
        "      \"id\": \"test_id\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name.png\"," +
        "      \"title\": \"test_title\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/400\"" +
        "    }," +
        "    {" +
        "      \"id\": \"test_id2\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name2.png\"," +
        "      \"title\": \"test_title2\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/test_name2.png\"" +
        "    }" +
        "  ]" +
        "}";
      runner.enqueue(data);
      runner.run(2);

      runner.assertQueueEmpty();
      runner.assertTransferCount(FetchSlack.REL_RESPONSE, 0);
      runner.assertTransferCount(FetchSlack.REL_SUCCESS_REQ, 0);
      runner.assertTransferCount(FetchSlack.REL_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_NO_RETRY, 1);
      runner.assertTransferCount(FetchSlack.REL_FAILURE, 0);

    } finally {
      testServer.shutdownServer();
    }
  }

  @Test
  public void testMultipleAttachementsWithFailureRetry() throws Exception {
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(TestContentServlet.class, "/*");

    TestServer testServer = new TestServer();
    testServer.addHandler(handler);
    try {
      testServer.startServer();
      String url = testServer.getUrl();


      String data = "{" +
        "  \"type\": \"message\"," +
        "  \"files\": [" +
        "    {" +
        "      \"id\": \"test_id\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name.png\"," +
        "      \"title\": \"test_title\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/500\"" +
        "    }," +
        "    {" +
        "      \"id\": \"test_id2\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name2.png\"," +
        "      \"title\": \"test_title2\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/test_name2.png\"" +
        "    }" +
        "  ]" +
        "}";
      runner.enqueue(data);
      runner.run(2);

      runner.assertQueueEmpty();
      runner.assertTransferCount(FetchSlack.REL_RESPONSE, 0);
      runner.assertTransferCount(FetchSlack.REL_SUCCESS_REQ, 0);
      runner.assertTransferCount(FetchSlack.REL_RETRY, 1);
      runner.assertTransferCount(FetchSlack.REL_NO_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_FAILURE, 0);

    } finally {
      testServer.shutdownServer();
    }
  }

  @Test
  public void testSingleAttachementSuccessCreatesProperOutput() throws Exception {
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(TestContentServlet.class, "/*");

    TestServer testServer = new TestServer();
    testServer.addHandler(handler);
    try {
      testServer.startServer();
      String url = testServer.getUrl();


      String data = "{" +
        "  \"type\": \"message\"," +
        "  \"files\": [" +
        "    {" +
        "      \"id\": \"test_id\"," +
        "      \"created\": 1550597907," +
        "      \"name\": \"test_name.png\"," +
        "      \"title\": \"test_title\"," +
        "      \"mimetype\": \"image/png\"," +
        "      \"filetype\": \"png\"," +
        "      \"size\": 1234," +
        "      \"url_private_download\": \"" + url + "/test_name.png\"" +
        "    }" +
        "  ]" +
        "}";
      runner.enqueue(data);
      runner.run(2);

      runner.assertQueueEmpty();
      runner.assertTransferCount(FetchSlack.REL_RESPONSE, 1);
      runner.assertTransferCount(FetchSlack.REL_SUCCESS_REQ, 1);
      runner.assertTransferCount(FetchSlack.REL_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_NO_RETRY, 0);
      runner.assertTransferCount(FetchSlack.REL_FAILURE, 0);

      List<MockFlowFile> response = runner.getFlowFilesForRelationship(FetchSlack.REL_RESPONSE);
      MockFlowFile flowFile = response.get(0);
      flowFile.assertContentEquals("test content");
      flowFile.assertAttributeEquals("message.name", "test_name.png");
      flowFile.assertAttributeEquals("message.title", "test_title");
      flowFile.assertAttributeEquals("message.mimetype", "image/png");
      flowFile.assertAttributeEquals("message.filetype", "png");
      flowFile.assertAttributeEquals("message.id", "test_id");
      flowFile.assertAttributeEquals("message.created", "1550597907");
      flowFile.assertAttributeEquals("message.size", "1234");
      flowFile.assertAttributeEquals("response.filename", "test_name.png");
      flowFile.assertAttributeEquals("response.content-length", "12");
      flowFile.assertAttributeEquals("response.content-type", "image/png");
      flowFile.assertAttributeEquals("response.date", "mockDate");


      List<MockFlowFile> original = runner.getFlowFilesForRelationship(FetchSlack.REL_SUCCESS_REQ);
      original.get(0).assertContentEquals(data);

    } finally {
      testServer.shutdownServer();
    }
  }


  public static class TestContentServlet extends HttpServlet {
    public TestContentServlet() {
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      String requestURI = req.getRequestURI().replace("/", "");
      if (requestURI.equals("400")) {
        resp.setStatus(400);
      } else if (requestURI.equals("500")) {
        resp.setStatus(500);
      } else {
        resp.setHeader("Content-Disposition", "attachment; filename=\"" + requestURI + "\"; filename*=UTF-8'" + requestURI);
        resp.setHeader("Content-Type", "image/png");
        resp.setHeader("Date", "mockDate");
        ServletOutputStream outputStream = resp.getOutputStream();
        outputStream.write("test content".getBytes());
      }
    }
  }
}