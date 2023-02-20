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
package org.apache.nifi.cdc.event.io;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;
import java.io.OutputStream;

public class EventWriterConfiguration {

    private final FlowFileEventWriteStrategy flowFileEventWriteStrategy;
    private final int numberOfEventsPerFlowFile;

    private int numberOfEventsWritten;

    private FlowFile currentFlowFile;
    private OutputStream flowFileOutputStream;
    private JsonGenerator jsonGenerator;

    public EventWriterConfiguration(FlowFileEventWriteStrategy flowFileEventWriteStrategy, int numberOfEventsPerFlowFile) {
        this.flowFileEventWriteStrategy = flowFileEventWriteStrategy;
        this.numberOfEventsPerFlowFile = numberOfEventsPerFlowFile;
    }

    public FlowFileEventWriteStrategy getFlowFileEventWriteStrategy() {
        return flowFileEventWriteStrategy;
    }

    public int getNumberOfEventsWritten() {
        return numberOfEventsWritten;
    }

    public void incrementNumberOfEventsWritten() {
        this.numberOfEventsWritten++;
    }

    public void startNewFlowFile(FlowFile flowFile, OutputStream flowFileOutputStream, JsonGenerator jsonGenerator) {
        this.currentFlowFile = flowFile;
        this.flowFileOutputStream = flowFileOutputStream;
        this.jsonGenerator = jsonGenerator;
    }

    public void cleanUp() throws IOException {
        this.currentFlowFile = null;
        this.flowFileOutputStream.close();
        this.flowFileOutputStream = null;
        this.jsonGenerator = null;
        this.numberOfEventsWritten = 0;
    }

    public int getNumberOfEventsPerFlowFile() {
        return numberOfEventsPerFlowFile;
    }

    public FlowFile getCurrentFlowFile() {
        return currentFlowFile;
    }

    public OutputStream getFlowFileOutputStream() {
        return flowFileOutputStream;
    }

    public JsonGenerator getJsonGenerator() {
        return jsonGenerator;
    }
}
