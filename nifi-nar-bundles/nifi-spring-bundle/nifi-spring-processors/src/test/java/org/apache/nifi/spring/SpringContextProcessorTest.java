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
package org.apache.nifi.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.spring.SpringDataExchanger.SpringResponse;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.Mockito;

public class SpringContextProcessorTest {

    @Test
    public void notValid() {
        TestRunner runner = TestRunners.newTestRunner(SpringContextProcessor.class);
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringContextProcessor.class);
        runner.setProperty(SpringContextProcessor.CTX_CONFIG_PATH, "context.xml");
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringContextProcessor.class);
        runner.setProperty(SpringContextProcessor.CTX_LIB_PATH, "fool");
        runner.assertNotValid();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOneWayFromNiFi() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.setProperty(SpringContextProcessor.CTX_CONFIG_PATH, "toSpringOnly.xml");
        runner.setProperty(SpringContextProcessor.CTX_LIB_PATH, ".");
        runner.setProperty(SpringContextProcessor.RECEIVE_TIMEOUT, "100 millis");
        runner.assertValid();
        runner.enqueue("Hello".getBytes());

        TestProcessor processor = (TestProcessor) runner.getProcessor();
        SpringDataExchanger delegate = processor.getMockedDelegate();
        when(delegate.receive(Mockito.anyLong())).thenReturn(null);
        when(delegate.send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong())).thenReturn(true);

        runner.run(1, false);
        verify(delegate, times(1)).send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong());
        verify(delegate, times(1)).receive(100);
        assertTrue(runner.getFlowFilesForRelationship(TestProcessor.REL_SUCCESS).isEmpty());
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOneWayFromSpring() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.setProperty(SpringContextProcessor.CTX_CONFIG_PATH, "fromSpringOnly.xml");
        runner.setProperty(SpringContextProcessor.CTX_LIB_PATH, ".");
        runner.assertValid();

        TestProcessor processor = (TestProcessor) runner.getProcessor();
        SpringDataExchanger delegate = processor.getMockedDelegate();
        SpringResponse<Object> r = new SpringResponse<Object>("hello".getBytes(),
                Collections.<String, Object> emptyMap());
        when(delegate.receive(Mockito.anyLong())).thenReturn(r);
        when(delegate.send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong())).thenReturn(true);

        runner.run(1, false);

        verify(delegate, never()).send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong());
        verify(delegate, times(1)).receive(0);
        assertTrue(runner.getFlowFilesForRelationship(TestProcessor.REL_SUCCESS).size() == 1);
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateBiDirectional() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.setProperty(SpringContextProcessor.CTX_CONFIG_PATH, "requestReply.xml");
        runner.setProperty(SpringContextProcessor.CTX_LIB_PATH, ".");
        runner.setProperty(SpringContextProcessor.RECEIVE_TIMEOUT, "100 millis");
        runner.assertValid();
        runner.enqueue("Hello".getBytes());

        TestProcessor processor = (TestProcessor) runner.getProcessor();
        SpringDataExchanger delegate = processor.getMockedDelegate();
        Map<String, Object> headers = new HashMap<>();
        headers.put("foo", "foo");
        headers.put("bar", new Object());
        SpringResponse<Object> r = new SpringResponse<Object>("hello".getBytes(), headers);
        when(delegate.receive(Mockito.anyLong())).thenReturn(r);
        when(delegate.send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong())).thenReturn(true);

        runner.run(1, false);
        verify(delegate, times(1)).send(Mockito.any(), Mockito.any(Map.class), Mockito.anyLong());
        verify(delegate, times(1)).receive(100);
        List<MockFlowFile> ffList = runner.getFlowFilesForRelationship(TestProcessor.REL_SUCCESS);
        assertTrue(ffList.size() == 1);
        assertEquals("foo", ffList.get(0).getAttribute("foo"));
        assertNull(ffList.get(0).getAttribute("bar"));
        runner.shutdown();
    }

    public static class TestProcessor extends SpringContextProcessor {
        private final SpringDataExchanger mockedDelegate = mock(SpringDataExchanger.class);

        public SpringDataExchanger getMockedDelegate() {
            return mockedDelegate;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
            try {
                Field ef = SpringContextProcessor.class.getDeclaredField("exchanger");
                ef.setAccessible(true);
                ef.set(this, this.mockedDelegate);
                super.onTrigger(context, processSession);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
