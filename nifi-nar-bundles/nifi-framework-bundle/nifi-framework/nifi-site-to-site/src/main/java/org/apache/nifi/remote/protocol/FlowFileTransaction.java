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
package org.apache.nifi.remote.protocol;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.StopWatch;

import java.util.Set;

public class FlowFileTransaction {

    private final ProcessSession session;
    private final ProcessContext context;
    private final StopWatch stopWatch;
    private final long bytesSent;
    private final Set<FlowFile> flowFilesSent;
    private final String calculatedCRC;

    public FlowFileTransaction() {
        this(null, null, new StopWatch(true), 0, null, null);
    }

    public FlowFileTransaction(ProcessSession session, ProcessContext context, StopWatch stopWatch, long bytesSent, Set<FlowFile> flowFilesSent, String calculatedCRC) {
        this.session = session;
        this.context = context;
        this.stopWatch = stopWatch;
        this.bytesSent = bytesSent;
        this.flowFilesSent = flowFilesSent;
        this.calculatedCRC = calculatedCRC;
    }

    public ProcessSession getSession() {
        return session;
    }

    public StopWatch getStopWatch() {
        return stopWatch;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public Set<FlowFile> getFlowFilesSent() {
        return flowFilesSent;
    }

    public String getCalculatedCRC() {
        return calculatedCRC;
    }

    public ProcessContext getContext() {
        return context;
    }
}
