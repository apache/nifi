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

package org.apache.nifi.processors.windows.event.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.W32Errors;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.windows.event.log.jna.EventSubscribeXmlRenderingCallback;
import org.apache.nifi.processors.windows.event.log.jna.WEvtApi;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockSessionFactory;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@RunWith(JNAJUnitRunner.class)
public class ConsumeWindowsEventLogTest {
    @Mock
    Kernel32 kernel32;

    @Mock
    WEvtApi wEvtApi;

    @Mock
    WinNT.HANDLE subscriptionHandle;

    @Mock
    Pointer subscriptionPointer;

    private ConsumeWindowsEventLog evtSubscribe;
    private TestRunner testRunner;

    public static List<WinNT.HANDLE> mockEventHandles(WEvtApi wEvtApi, Kernel32 kernel32, List<String> eventXmls) {
        List<WinNT.HANDLE> eventHandles = new ArrayList<>();
        for (String eventXml : eventXmls) {
            WinNT.HANDLE eventHandle = mock(WinNT.HANDLE.class);
            when(wEvtApi.EvtRender(isNull(WinNT.HANDLE.class), eq(eventHandle), eq(WEvtApi.EvtRenderFlags.EVENT_XML),
                    anyInt(), any(Pointer.class), any(Pointer.class), any(Pointer.class))).thenAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                Pointer bufferUsed = (Pointer) arguments[5];
                byte[] array = StandardCharsets.UTF_16LE.encode(eventXml).array();
                if (array.length > (int) arguments[3]) {
                    when(kernel32.GetLastError()).thenReturn(W32Errors.ERROR_INSUFFICIENT_BUFFER).thenReturn(W32Errors.ERROR_SUCCESS);
                } else {
                    ((Pointer) arguments[4]).write(0, array, 0, array.length);
                }
                bufferUsed.setInt(0, array.length);
                return false;
            });
            eventHandles.add(eventHandle);
        }
        return eventHandles;
    }

    @Before
    public void setup() {
        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);


        when(subscriptionHandle.getPointer()).thenReturn(subscriptionPointer);

        when(wEvtApi.EvtSubscribe(isNull(WinNT.HANDLE.class), isNull(WinNT.HANDLE.class), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(WinNT.HANDLE.class), isNull(WinDef.PVOID.class), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(subscriptionHandle);

        testRunner = TestRunners.newTestRunner(evtSubscribe);
    }

    @Test(timeout = 10 * 1000)
    public void testProcessesBlockedEvents() throws UnsupportedEncodingException {
        testRunner.setProperty(ConsumeWindowsEventLog.MAX_EVENT_QUEUE_SIZE, "1");
        testRunner.run(1, false, true);
        EventSubscribeXmlRenderingCallback renderingCallback = getRenderingCallback();

        List<String> eventXmls = Arrays.asList("one", "two", "three", "four", "five", "six");
        List<WinNT.HANDLE> eventHandles = mockEventHandles(wEvtApi, kernel32, eventXmls);
        AtomicBoolean done = new AtomicBoolean(false);
        new Thread(() -> {
            for (WinNT.HANDLE eventHandle : eventHandles) {
                renderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, eventHandle);
            }
            done.set(true);
        }).start();

        // Wait until the thread has really started
        while (testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS).size() == 0) {
            testRunner.run(1, false, false);
        }

        // Process rest of events
        while (!done.get()) {
            testRunner.run(1, false, false);
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS);
        assertEquals(eventXmls.size(), flowFilesForRelationship.size());
        for (int i = 0; i < eventXmls.size(); i++) {
            flowFilesForRelationship.get(i).assertContentEquals(eventXmls.get(i));
        }
    }

    @Test
    public void testStopProcessesQueue() throws InvocationTargetException, IllegalAccessException {
        testRunner.run(1, false);

        List<String> eventXmls = Arrays.asList("one", "two", "three");
        for (WinNT.HANDLE eventHandle : mockEventHandles(wEvtApi, kernel32, eventXmls)) {
            getRenderingCallback().onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, eventHandle);
        }

        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, evtSubscribe, testRunner.getProcessContext());

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS);
        assertEquals(eventXmls.size(), flowFilesForRelationship.size());
        for (int i = 0; i < eventXmls.size(); i++) {
            flowFilesForRelationship.get(i).assertContentEquals(eventXmls.get(i));
        }
    }

    @Test
    public void testScheduleErrorThenTriggerSubscribe() throws InvocationTargetException, IllegalAccessException {
        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);


        when(subscriptionHandle.getPointer()).thenReturn(subscriptionPointer);

        when(wEvtApi.EvtSubscribe(isNull(WinNT.HANDLE.class), isNull(WinNT.HANDLE.class), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(WinNT.HANDLE.class), isNull(WinDef.PVOID.class), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(null).thenReturn(subscriptionHandle);

        testRunner = TestRunners.newTestRunner(evtSubscribe);


        testRunner.run(1, false, true);

        WinNT.HANDLE handle = mockEventHandles(wEvtApi, kernel32, Arrays.asList("test")).get(0);
        List<EventSubscribeXmlRenderingCallback> renderingCallbacks = getRenderingCallbacks(2);
        EventSubscribeXmlRenderingCallback subscribeRenderingCallback = renderingCallbacks.get(0);
        EventSubscribeXmlRenderingCallback renderingCallback = renderingCallbacks.get(1);
        renderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);

        testRunner.run(1, true, false);

        assertNotEquals(subscribeRenderingCallback, renderingCallback);
        verify(wEvtApi).EvtClose(subscriptionHandle);
    }

    @Test
    public void testScheduleError() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);

        when(wEvtApi.EvtSubscribe(isNull(WinNT.HANDLE.class), isNull(WinNT.HANDLE.class), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(WinNT.HANDLE.class), isNull(WinDef.PVOID.class), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(null);

        when(kernel32.GetLastError()).thenReturn(WinError.ERROR_ACCESS_DENIED);

        testRunner = TestRunners.newTestRunner(evtSubscribe);

        testRunner.run(1);
        assertEquals(0, getCreatedSessions(testRunner).size());
        verify(wEvtApi, never()).EvtClose(any(WinNT.HANDLE.class));
    }

    @Test
    public void testStopClosesHandle() {
        testRunner.run(1);
        verify(wEvtApi).EvtClose(subscriptionHandle);
    }

    @Test(expected = ProcessException.class)
    public void testScheduleQueueStopThrowsException() throws Throwable {
        ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, evtSubscribe, testRunner.getProcessContext());

        WinNT.HANDLE handle = mockEventHandles(wEvtApi, kernel32, Arrays.asList("test")).get(0);
        getRenderingCallback().onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, evtSubscribe, testRunner.getProcessContext());
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    public EventSubscribeXmlRenderingCallback getRenderingCallback() {
        return getRenderingCallbacks(1).get(0);
    }

    public List<EventSubscribeXmlRenderingCallback> getRenderingCallbacks(int times) {
        ArgumentCaptor<EventSubscribeXmlRenderingCallback> callbackArgumentCaptor = ArgumentCaptor.forClass(EventSubscribeXmlRenderingCallback.class);
        verify(wEvtApi, times(times)).EvtSubscribe(isNull(WinNT.HANDLE.class), isNull(WinNT.HANDLE.class), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(WinNT.HANDLE.class), isNull(WinDef.PVOID.class), callbackArgumentCaptor.capture(),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT));
        return callbackArgumentCaptor.getAllValues();
    }

    @Test
    public void testGetSupportedPropertyDescriptors() {
        assertEquals(ConsumeWindowsEventLog.PROPERTY_DESCRIPTORS, evtSubscribe.getSupportedPropertyDescriptors());
    }

    @Test
    public void testGetRelationships() {
        assertEquals(ConsumeWindowsEventLog.RELATIONSHIPS, evtSubscribe.getRelationships());
    }

    private static Set<MockProcessSession> getCreatedSessions(TestRunner testRunner) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MockSessionFactory processSessionFactory = (MockSessionFactory) testRunner.getProcessSessionFactory();
        Method getCreatedSessions = processSessionFactory.getClass().getDeclaredMethod("getCreatedSessions");
        getCreatedSessions.setAccessible(true);
        return (Set<MockProcessSession>) getCreatedSessions.invoke(processSessionFactory);
    }
}
