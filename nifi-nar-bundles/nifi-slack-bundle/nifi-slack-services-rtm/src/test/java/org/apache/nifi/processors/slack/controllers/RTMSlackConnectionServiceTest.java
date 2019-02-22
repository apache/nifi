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
package org.apache.nifi.processors.slack.controllers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.github.seratch.jslack.api.rtm.RTMClient;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.nifi.processor.Processor;
import org.junit.Before;
import org.junit.Test;

public class RTMSlackConnectionServiceTest {

  private RTMSlackConnectionService serviceMock;
  private RTMClient clientMock;
  private Processor processorMock;

  @Before
  public void setUp() throws IOException {
    clientMock = mock(RTMClient.class);

    serviceMock = mock(RTMSlackConnectionService.class, withSettings().useConstructor());
    when(serviceMock.getRtmClient(any())).thenReturn(clientMock);
    doCallRealMethod().when(serviceMock).registerProcessor(any(), any());
    doCallRealMethod().when(serviceMock).deregisterProcessor(any());
    doCallRealMethod().when(serviceMock).isProcessorRegistered(any());
    doCallRealMethod().when(serviceMock).sendMessage(any());

    processorMock = mock(Processor.class);
    when(processorMock.getIdentifier()).thenReturn("test-processor");
  }

  @Test
  public void testRegistering() throws IOException {
    assertFalse(serviceMock.isProcessorRegistered(processorMock));
    serviceMock.registerProcessor(processorMock, s -> {});
    assertTrue(serviceMock.isProcessorRegistered(processorMock));
    serviceMock.deregisterProcessor(processorMock);
    assertFalse(serviceMock.isProcessorRegistered(processorMock));
  }

  @Test
  public void testRegisteredHandlerGetsCalled() {
    Consumer consumer = mock(Consumer.class);
    String testString = "test";
    serviceMock.registerProcessor(processorMock, consumer);
    serviceMock.sendMessage(testString);

    verify(consumer, times(1)).accept(testString);
  }

}