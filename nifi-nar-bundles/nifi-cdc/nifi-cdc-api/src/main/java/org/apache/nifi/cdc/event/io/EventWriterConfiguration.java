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
import org.apache.nifi.processor.ProcessSession;

import java.io.OutputStream;

public class EventWriterConfiguration {

    private FlowFileEventWriteStrategy flowFileEventWriteStrategy;
    private int numberOfEventsWritten = 0;
    private int numberOfEventsPerFlowFile = 1000;
    private FlowFile currentFlowFile;
    private OutputStream flowFileOutputStream;
    private ProcessSession workingSession;
    private JsonGenerator jsonGenerator;

    public EventWriterConfiguration(FlowFileEventWriteStrategy flowFileEventWriteStrategy, int numberOfEventsWritten, int numberOfEventsPerFlowFile, FlowFile currentFlowFile) {
        this.flowFileEventWriteStrategy = flowFileEventWriteStrategy;
        this.numberOfEventsWritten = numberOfEventsWritten;
        this.numberOfEventsPerFlowFile = numberOfEventsPerFlowFile;
        this.currentFlowFile = currentFlowFile;
    }

    public FlowFileEventWriteStrategy getFlowFileEventWriteStrategy() {
        return flowFileEventWriteStrategy;
    }

    public void setFlowFileEventWriteStrategy(FlowFileEventWriteStrategy flowFileEventWriteStrategy) {
        this.flowFileEventWriteStrategy = flowFileEventWriteStrategy;
    }

    public int getNumberOfEventsWritten() {
        return numberOfEventsWritten;
    }

    public void setNumberOfEventsWritten(int numberOfEventsWritten) {
        this.numberOfEventsWritten = numberOfEventsWritten;
    }

    public void incrementNumberOfEventsWritten() {
        this.numberOfEventsWritten++;
    }

    public int getNumberOfEventsPerFlowFile() {
        return numberOfEventsPerFlowFile;
    }

    public void setNumberOfEventsPerFlowFile(int numberOfEventsPerFlowFile) {
        this.numberOfEventsPerFlowFile = numberOfEventsPerFlowFile;
    }

    public FlowFile getCurrentFlowFile() {
        return currentFlowFile;
    }

    public void setCurrentFlowFile(FlowFile currentFlowFile) {
        this.currentFlowFile = currentFlowFile;
    }

    public OutputStream getFlowFileOutputStream() {
        return flowFileOutputStream;
    }

    public void setFlowFileOutputStream(OutputStream flowFileOutputStream) {
        this.flowFileOutputStream = flowFileOutputStream;
    }


    public ProcessSession getWorkingSession() {
        return workingSession;
    }

    public void setWorkingSession(ProcessSession workingSession) {
        this.workingSession = workingSession;
    }

    public JsonGenerator getJsonGenerator() {
        return jsonGenerator;
    }

    public void setJsonGenerator(JsonGenerator jsonGenerator) {
        this.jsonGenerator = jsonGenerator;
    }
}
