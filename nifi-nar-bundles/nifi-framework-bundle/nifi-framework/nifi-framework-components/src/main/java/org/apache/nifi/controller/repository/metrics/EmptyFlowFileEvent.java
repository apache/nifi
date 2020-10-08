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
package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;

import java.util.Collections;
import java.util.Map;

public class EmptyFlowFileEvent implements FlowFileEvent {
    public static final EmptyFlowFileEvent INSTANCE = new EmptyFlowFileEvent();

    private EmptyFlowFileEvent() {
    }

    @Override
    public int getFlowFilesIn() {
        return 0;
    }

    @Override
    public int getFlowFilesOut() {
        return 0;
    }

    @Override
    public int getFlowFilesRemoved() {
        return 0;
    }

    @Override
    public long getContentSizeIn() {
        return 0;
    }

    @Override
    public long getContentSizeOut() {
        return 0;
    }

    @Override
    public long getContentSizeRemoved() {
        return 0;
    }

    @Override
    public long getBytesRead() {
        return 0;
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public long getProcessingNanoseconds() {
        return 0;
    }

    @Override
    public long getAverageLineageMillis() {
        return 0;
    }

    @Override
    public long getAggregateLineageMillis() {
        return 0;
    }

    @Override
    public int getFlowFilesReceived() {
        return 0;
    }

    @Override
    public long getBytesReceived() {
        return 0;
    }

    @Override
    public int getFlowFilesSent() {
        return 0;
    }

    @Override
    public long getBytesSent() {
        return 0;
    }

    @Override
    public int getInvocations() {
        return 0;
    }

    @Override
    public Map<String, Long> getCounters() {
        return Collections.emptyMap();
    }
}
