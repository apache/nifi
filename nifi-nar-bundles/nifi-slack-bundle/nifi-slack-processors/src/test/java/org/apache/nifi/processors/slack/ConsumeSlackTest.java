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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.nifi.processors.slack.controllers.SlackConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ConsumeSlackTest {

  private static final String SERVICE_ID = "testId";
  private static final List<String> INPUT_MESSAGES = Arrays.asList(
    "{\"type\":\"hello\"}",
    "{\"type\":\"message\"}",
    "{\"type\":\"desktop_notification\"}",
    "{\"type\":\"file_shared\"}",
    "",
    "{}"
  );
  private ConsumeSlack consumeSlack;
  private TestRunner runner;
  private SlackConnectionService slackConnectionService;
  private CompletableFuture<Consumer<String>> handlerFuture;

  @Before
  public void setUp() throws InitializationException {
    consumeSlack = new ConsumeSlack();
    runner = TestRunners.newTestRunner(consumeSlack);
    slackConnectionService = mock(SlackConnectionService.class);

    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
    when(slackConnectionService.isProcessorRegistered(consumeSlack)).thenReturn(true);

    runner.setProperty(ConsumeSlack.SLACK_CONNECTION_SERVICE, SERVICE_ID);
    runner.setProperty(ConsumeSlack.MAX_MESSAGE_QUEUE_SIZE, "10");
    runner.addControllerService(SERVICE_ID, slackConnectionService);
    runner.enableControllerService(slackConnectionService);

    handlerFuture = new CompletableFuture<>();
    doAnswer(invocation -> {
      handlerFuture.complete(invocation.getArgumentAt(1, Consumer.class));
      return null;
    }).when(slackConnectionService).registerProcessor(eq(consumeSlack), any());


  }

  @Test
  public void testEmptyMessageTypesMatchesAnything() throws ExecutionException, InterruptedException {

    runner.run(1, false);
    INPUT_MESSAGES.forEach(handlerFuture.get());
    runner.run(10, false, false);

    runner.assertAllFlowFilesTransferred(ConsumeSlack.SUCCESS_RELATIONSHIP, INPUT_MESSAGES.size());

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ConsumeSlack.SUCCESS_RELATIONSHIP);
    for (int i = 0; i < INPUT_MESSAGES.size(); i++) {
      matched.get(i).assertContentEquals(INPUT_MESSAGES.get(i));
    }
  }


  @Test
  public void testMessageTypeMatchesProperMessages() throws ExecutionException, InterruptedException {

    runner.setProperty(ConsumeSlack.MESSAGE_TYPES, "message");
    runner.run(1, false);
    INPUT_MESSAGES.forEach(handlerFuture.get());
    runner.run(10, false, false);

    runner.assertAllFlowFilesTransferred(ConsumeSlack.SUCCESS_RELATIONSHIP, 1);

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ConsumeSlack.SUCCESS_RELATIONSHIP);
    matched.get(0).assertContentEquals(INPUT_MESSAGES.get(1));
  }


  @Test
  public void testMultipleMessageTypesMatchesProperMessages() throws ExecutionException, InterruptedException {

    runner.setProperty(ConsumeSlack.MESSAGE_TYPES, "message,file_shared");
    runner.run(1, false);
    INPUT_MESSAGES.forEach(handlerFuture.get());
    runner.run(10, false, false);

    runner.assertAllFlowFilesTransferred(ConsumeSlack.SUCCESS_RELATIONSHIP, 2);

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ConsumeSlack.SUCCESS_RELATIONSHIP);
    matched.get(0).assertContentEquals(INPUT_MESSAGES.get(1));
    matched.get(1).assertContentEquals(INPUT_MESSAGES.get(3));
  }

  @Test
  public void testRegisterAndDeregisterHappens() {
    runner.run();

    verify(slackConnectionService, times(1)).isProcessorRegistered(eq(consumeSlack));
    verify(slackConnectionService, times(1)).registerProcessor(eq(consumeSlack), any());
    verify(slackConnectionService, times(1)).deregisterProcessor(eq(consumeSlack));
  }

  @Test
  public void testBackPressure() throws ExecutionException, InterruptedException {
    runner.setProperty(ConsumeSlack.MAX_MESSAGE_QUEUE_SIZE, "1");
    runner.run(1, false);
    INPUT_MESSAGES.forEach(handlerFuture.get());
    runner.run(10, false, false);

    runner.assertAllFlowFilesTransferred(ConsumeSlack.SUCCESS_RELATIONSHIP, 1);
  }
}