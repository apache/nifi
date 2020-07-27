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
package org.apache.nifi.processor.util.pattern;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutingResult {

    private final Map<Relationship, List<FlowFile>> routedFlowFiles = new HashMap<>();

    public void routeTo(final FlowFile flowFile, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).add(flowFile);
    }

    public void routeTo(final List<FlowFile> flowFiles, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).addAll(flowFiles);
    }

    public void merge(final RoutingResult r) {
        r.getRoutedFlowFiles().forEach((relationship, routedFlowFiles) -> routeTo(routedFlowFiles, relationship));
    }

    public Map<Relationship, List<FlowFile>> getRoutedFlowFiles() {
        return routedFlowFiles;
    }

    public boolean contains(Relationship relationship) {
        return routedFlowFiles.containsKey(relationship) && !routedFlowFiles.get(relationship).isEmpty();
    }
}
