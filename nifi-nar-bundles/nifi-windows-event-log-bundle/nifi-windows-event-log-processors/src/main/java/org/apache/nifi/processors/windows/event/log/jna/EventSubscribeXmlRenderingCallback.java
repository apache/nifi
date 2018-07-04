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

import com.sun.jna.Memory;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.W32Errors;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinNT;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.apache.nifi.logging.ComponentLog;

/**
 * Callback that will render the XML representation of the event using native Windows API
 */
public class EventSubscribeXmlRenderingCallback implements WEvtApi.EVT_SUBSCRIBE_CALLBACK {
    public static final String RECEIVED_THE_FOLLOWING_WIN32_ERROR = "Received the following Win32 error: ";
    public static final int INITIAL_BUFFER_SIZE = 1024;
    public static final String EVT_RENDER_RETURNED_THE_FOLLOWING_ERROR_CODE = "EvtRender returned the following error code ";
    public static final String MISSING_EVENT_MESSAGE = "Received missing event notification.  Consider triggering processor more frequently or increasing queue size.";

    private final ComponentLog logger;
    private final Consumer<String> consumer;
    private final int maxBufferSize;
    private final WEvtApi wEvtApi;
    private final Kernel32 kernel32;
    private final ErrorLookup errorLookup;

    private int size;
    private Memory buffer;
    private Memory used;
    private Memory propertyCount;

    public EventSubscribeXmlRenderingCallback(ComponentLog logger, Consumer<String> consumer, int maxBufferSize, WEvtApi wEvtApi, Kernel32 kernel32, ErrorLookup errorLookup) {
        this.logger = logger;
        this.consumer = consumer;
        this.maxBufferSize = maxBufferSize;
        this.wEvtApi = wEvtApi;
        this.kernel32 = kernel32;
        this.size = Math.min(maxBufferSize, INITIAL_BUFFER_SIZE);
        this.errorLookup = errorLookup;
        this.buffer = new Memory(size);
        this.used = new Memory(4);
        this.propertyCount = new Memory(4);
    }

    @Override
    public synchronized int onEvent(int evtSubscribeNotifyAction, WinDef.PVOID userContext, WinNT.HANDLE eventHandle) {
        if (logger.isDebugEnabled()) {
            logger.debug("onEvent(" + evtSubscribeNotifyAction + ", " + userContext + ", " + eventHandle);
        }

        if (evtSubscribeNotifyAction == WEvtApi.EvtSubscribeNotifyAction.ERROR) {
            if (eventHandle.getPointer().getInt(0) == WEvtApi.EvtSubscribeErrors.ERROR_EVT_QUERY_RESULT_STALE) {
                logger.error(MISSING_EVENT_MESSAGE);
            } else {
                logger.error(RECEIVED_THE_FOLLOWING_WIN32_ERROR + eventHandle.getPointer().getInt(0));
            }
        } else if (evtSubscribeNotifyAction == WEvtApi.EvtSubscribeNotifyAction.DELIVER) {
            wEvtApi.EvtRender(null, eventHandle, WEvtApi.EvtRenderFlags.EVENT_XML, size, buffer, used, propertyCount);

            // Not enough room in buffer, resize so it's big enough
            if (kernel32.GetLastError() == W32Errors.ERROR_INSUFFICIENT_BUFFER) {
                int newMaxSize = used.getInt(0);
                // Check for overflow or too big
                if (newMaxSize < size || newMaxSize > maxBufferSize) {
                    logger.error("Dropping event " + eventHandle + " because it couldn't be rendered within " + maxBufferSize + " bytes.");
                    // Ignored, see https://msdn.microsoft.com/en-us/library/windows/desktop/aa385577(v=vs.85).aspx
                    return 0;
                }
                size = newMaxSize;
                buffer = new Memory(size);
                wEvtApi.EvtRender(null, eventHandle, WEvtApi.EvtRenderFlags.EVENT_XML, size, buffer, used, propertyCount);
            }

            int lastError = kernel32.GetLastError();
            if (lastError == W32Errors.ERROR_SUCCESS) {
                int usedBytes = used.getInt(0);
                String string = StandardCharsets.UTF_16LE.decode(buffer.getByteBuffer(0, usedBytes)).toString();
                if (string.endsWith("\u0000")) {
                    string = string.substring(0, string.length() - 1);
                }
                consumer.accept(string);
            } else {
                logger.error(EVT_RENDER_RETURNED_THE_FOLLOWING_ERROR_CODE + errorLookup.getLastError() + ".");
            }
        }
        // Ignored, see https://msdn.microsoft.com/en-us/library/windows/desktop/aa385577(v=vs.85).aspx
        return 0;
    }
}
