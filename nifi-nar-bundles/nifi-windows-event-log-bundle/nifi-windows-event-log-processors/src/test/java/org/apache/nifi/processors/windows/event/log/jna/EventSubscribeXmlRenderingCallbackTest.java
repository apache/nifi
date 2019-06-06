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

package org.apache.nifi.processors.windows.event.log.jna;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.windows.event.log.ConsumeWindowsEventLogTest;
import org.apache.nifi.processors.windows.event.log.JNAJUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JNAJUnitRunner.class)
public class EventSubscribeXmlRenderingCallbackTest {
    @Mock
    ComponentLog logger;

    @Mock
    Consumer<String> consumer;

    @Mock
    WEvtApi wEvtApi;

    @Mock
    Kernel32 kernel32;

    @Mock
    ErrorLookup errorLookup;

    @Mock
    WinNT.HANDLE handle;

    private EventSubscribeXmlRenderingCallback eventSubscribeXmlRenderingCallback;
    private int maxBufferSize;

    @Before
    public void setup() {
        maxBufferSize = 8;
        eventSubscribeXmlRenderingCallback = new EventSubscribeXmlRenderingCallback(logger, consumer, maxBufferSize, wEvtApi, kernel32, errorLookup);
    }

    @Test
    public void testErrorJustLogs() {
        int errorCode = 111;
        Pointer pointer = mock(Pointer.class);
        when(handle.getPointer()).thenReturn(pointer);
        when(pointer.getInt(0)).thenReturn(errorCode);

        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.ERROR, null, handle);
        verify(logger).error(EventSubscribeXmlRenderingCallback.RECEIVED_THE_FOLLOWING_WIN32_ERROR + errorCode);
    }

    @Test
    public void testMissingRecordLog() {
        Pointer pointer = mock(Pointer.class);
        when(handle.getPointer()).thenReturn(pointer);
        when(pointer.getInt(0)).thenReturn(WEvtApi.EvtSubscribeErrors.ERROR_EVT_QUERY_RESULT_STALE);

        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.ERROR, null, handle);
        verify(logger).error(EventSubscribeXmlRenderingCallback.MISSING_EVENT_MESSAGE);
    }

    @Test
    public void testSuccessfulRender() {
        String small = "abc";
        handle = ConsumeWindowsEventLogTest.mockEventHandles(wEvtApi, kernel32, Arrays.asList(small + "\u0000")).get(0);
        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);
        verify(consumer).accept(small);
    }

    @Test
    public void testUnsuccessfulRender() {
        String large = "abcde";
        handle = ConsumeWindowsEventLogTest.mockEventHandles(wEvtApi, kernel32, Arrays.asList(large)).get(0);
        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);
        verify(consumer, never()).accept(anyString());
    }

    @Test
    public void testResizeRender() {
        // Make a string too big to fit into initial buffer
        StringBuilder testStringBuilder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            testStringBuilder.append(i);
        }
        String base = testStringBuilder.toString();
        testStringBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            testStringBuilder.append(base);
        }
        String veryLarge = testStringBuilder.toString();

        handle = ConsumeWindowsEventLogTest.mockEventHandles(wEvtApi, kernel32, Arrays.asList(veryLarge)).get(0);
        eventSubscribeXmlRenderingCallback = new EventSubscribeXmlRenderingCallback(logger, consumer, 2048, wEvtApi, kernel32, errorLookup);
        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);
        verify(consumer).accept(veryLarge);
    }

    @Test
    public void testErrorRendering() {
        int value = 225;
        String code = "225code";
        when(kernel32.GetLastError()).thenReturn(value);
        when(errorLookup.getLastError()).thenReturn(code);
        eventSubscribeXmlRenderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);
        verify(logger).error(EventSubscribeXmlRenderingCallback.EVT_RENDER_RETURNED_THE_FOLLOWING_ERROR_CODE + code + ".");
    }
}
