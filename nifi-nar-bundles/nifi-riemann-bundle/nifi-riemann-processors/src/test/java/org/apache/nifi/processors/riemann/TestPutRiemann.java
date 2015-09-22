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
package org.apache.nifi.processors.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.RiemannClient;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutRiemann {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  // Holds incoming events to Riemann
  private Queue<Proto.Event> eventStream = new LinkedList<Proto.Event>();

  @Before
  public void clearEventStream() {
    eventStream.clear();
  }

  private TestRunner getTestRunner() {
    return getTestRunner(false);
  }

  private TestRunner getTestRunner(final boolean failOnWrite) {
    RiemannClient riemannClient = mock(RiemannClient.class);
    when(riemannClient.sendEvents(anyListOf(Proto.Event.class))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<Proto.Event> events = (List<Proto.Event>) invocationOnMock.getArguments()[0];
        for (Proto.Event event : events) {
          eventStream.add(event);
        }
        IPromise iPromise = mock(IPromise.class);
        if (!failOnWrite) {
          when(iPromise.deref(anyInt(), any(TimeUnit.class))).thenReturn(Proto.Msg.getDefaultInstance());
        } else {
          when(iPromise.deref(anyInt(), any(TimeUnit.class))).thenReturn(null);
        }
        return iPromise;
      }
    });
    when(riemannClient.isConnected()).thenReturn(true);
    PutRiemann riemannProcessor = new PutRiemann();
    riemannProcessor.riemannClient = riemannClient;
    riemannProcessor.transport = PutRiemann.Transport.TCP;

    TestRunner runner = TestRunners.newTestRunner(riemannProcessor);
    runner.setProperty(PutRiemann.RIEMANN_HOST, "localhost");
    runner.setProperty(PutRiemann.RIEMANN_PORT, "5555");
    runner.setProperty(PutRiemann.TRANSPORT_PROTOCOL, "TCP");
    runner.setProperty(PutRiemann.BATCH_SIZE, "100");
    runner.setProperty(PutRiemann.ATTR_SERVICE, "nifi-test-service");
    runner.setProperty(PutRiemann.ATTR_HOST, "${riemann.host}");
    runner.setProperty(PutRiemann.ATTR_TTL, "5");
    runner.setProperty(PutRiemann.ATTR_DESCRIPTION, "test");
    runner.setProperty(PutRiemann.ATTR_TAGS, "tag1, tag2, tag3");
    runner.setProperty(PutRiemann.ATTR_METRIC, "${riemann.metric}");
    runner.setProperty("custom-attribute-1", "${custom.attribute.1}");
    runner.setProperty("custom-attribute-2", "${custom.attribute.2}");
    runner.setProperty("custom-attribute-3", "${custom.attribute.3}");
    return runner;
  }


  @Test
  public void testBasicEvent() {
    TestRunner runner = getTestRunner();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("riemann.metric", "42");
    attributes.put("riemann.host", "basic-host");
    MockFlowFile flowFile = new MockFlowFile(1);
    flowFile.putAttributes(attributes);
    runner.enqueue(flowFile);
    runner.run();
    runner.assertAllFlowFilesTransferred(PutRiemann.REL_SUCCESS);

    Proto.Event event = eventStream.remove();
    assertEquals("nifi-test-service", event.getService());
    assertTrue(5.0 == event.getTtl());
    assertTrue(42.0 == event.getMetricF());
    assertEquals("basic-host", event.getHost());
    assertEquals("test", event.getDescription());
    assertEquals(3, event.getTagsCount());
    assertTrue(event.getTagsList().contains("tag1"));
    assertTrue(event.getTagsList().contains("tag2"));
    assertTrue(event.getTagsList().contains("tag3"));
    assertEquals(0, event.getAttributesCount());
  }

  @Test
  public void testBatchedEvents() {
    // (2 batches) + (1 remaining event)
    int iterations = Integer.parseInt(PutRiemann.BATCH_SIZE.getDefaultValue()) * 2 + 1;
    TestRunner runner = getTestRunner();

    for (int i = 0; i < iterations; i++) {
      Map<String, String> attributes = new HashMap<>();
      attributes.put("riemann.metric", Float.toString(i));
      attributes.put("riemann.host", "batch-host");
      attributes.put("custom.attribute.1", "attr1");
      attributes.put("custom.attribute.2", "attr2");
      attributes.put("custom.attribute.3", "attr3");
      MockFlowFile flowFile = new MockFlowFile(i);
      flowFile.putAttributes(attributes);
      runner.enqueue(flowFile);
    }
    runner.run(3);
    runner.assertAllFlowFilesTransferred(PutRiemann.REL_SUCCESS);

    for (int i = 0; i < iterations; i++) {
      Proto.Event event = eventStream.remove();
      assertEquals("nifi-test-service", event.getService());
      assertTrue(5.0 == event.getTtl());
      assertTrue(i == event.getMetricF());
      assertEquals("batch-host", event.getHost());
      assertEquals("test", event.getDescription());
      assertEquals(3, event.getTagsCount());
      assertEquals(3, event.getAttributesCount());
      assertTrue(event.getTagsList().contains("tag1"));
      assertTrue(event.getTagsList().contains("tag2"));
      assertTrue(event.getTagsList().contains("tag3"));
    }
  }

  @Test
  public void testInvalidEvents() {
    TestRunner runner = getTestRunner();
    MockFlowFile flowFile = new MockFlowFile(1);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("riemann.metric", "NOT A NUMBER");
    flowFile.putAttributes(attributes);
    runner.enqueue(flowFile);
    runner.run();
    runner.assertAllFlowFilesTransferred(PutRiemann.REL_FAILURE);
  }


  @Test(expected = AssertionError.class)
  public void testFailedDeref() {
    TestRunner runner = getTestRunner(true);
    MockFlowFile flowFile = new MockFlowFile(1);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("riemann.metric", "5");
    flowFile.putAttributes(attributes);
    runner.enqueue(flowFile);
    try {
      runner.run();
    } catch (ProcessException e) {
      runner.assertQueueNotEmpty();
      throw e;
    }
  }
}
