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
package org.apache.nifi.controller.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the status of the processor data transfers as stored in the repository.
 */
public class StandardRepositoryStatusReport implements RepositoryStatusReport {

    public StandardRepositoryStatusReport() {
    }

    /**
     * Returns a map where the key is the processor ID and the value is the status entry for the processor.
     *
     * @return a map of report entries
     */
    @Override
    public Map<String, FlowFileEvent> getReportEntries() {
        return Collections.unmodifiableMap(entries);
    }

    /**
     * Returns a particular entry for a given processor ID. If the processor ID does not exist, then null is returned.
     *
     * @param componentId the ID of a component; that component may be a Processor, a Connection, a ProcessGroup, etc.
     *
     * @return a status entry
     */
    @Override
    public FlowFileEvent getReportEntry(final String componentId) {
        return entries.get(componentId);
    }

    /**
     * Adds an entry to the report.
     *
     * @param entry an entry
     */
    @Override
    public void addReportEntry(FlowFileEvent entry) {
        if (entry == null) {
            throw new NullPointerException("report entry may not be null");
        }
        this.entries.put(entry.getComponentIdentifier(), entry);
    }

    @Override
    public String toString() {
        final StringBuilder strb = new StringBuilder();
        for (final String key : this.entries.keySet()) {
            final FlowFileEvent entry = this.entries.get(key);
            strb.append("[")
                    .append(entry.getComponentIdentifier()).append(", ")
                    .append(entry.getFlowFilesIn()).append(", ")
                    .append(entry.getContentSizeIn()).append(", ")
                    .append(entry.getFlowFilesOut()).append(", ")
                    .append(entry.getContentSizeOut()).append(", ")
                    .append(entry.getBytesRead()).append(", ")
                    .append(entry.getBytesWritten()).append("]\n");
        }
        return strb.toString();
    }

    private final Map<String, FlowFileEvent> entries = new HashMap<>();
}
