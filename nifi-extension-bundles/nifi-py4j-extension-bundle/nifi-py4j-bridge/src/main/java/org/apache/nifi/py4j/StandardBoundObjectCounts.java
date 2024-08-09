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

package org.apache.nifi.py4j;

import org.apache.nifi.python.BoundObjectCounts;

import java.util.Map;

public class StandardBoundObjectCounts implements BoundObjectCounts {
    private final String process;
    private final String processorType;
    private final String processorVersion;
    private final Map<String, Integer> counts;

    public StandardBoundObjectCounts(final String process, final String processorType, final String processorVersion, final Map<String, Integer> counts) {
        this.process = process;
        this.processorType = processorType;
        this.processorVersion = processorVersion;
        this.counts = counts;
    }

    @Override
    public String getProcess() {
        return process;
    }

    @Override
    public String getProcessorType() {
        return processorType;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    @Override
    public Map<String, Integer> getCounts() {
        return counts;
    }
}
