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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

public class FlowFileSessionWrapper {

    private final FlowFile flowFile;
    private final ProcessSession session;

    public FlowFileSessionWrapper(final FlowFile flowFile, final ProcessSession session) {
        this.flowFile = flowFile;
        this.session = session;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public ProcessSession getSession() {
        return session;
    }

    @Override
    public String toString() {
        return flowFile.toString();
    }
}
