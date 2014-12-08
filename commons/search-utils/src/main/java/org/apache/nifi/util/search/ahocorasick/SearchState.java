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
package org.apache.nifi.util.search.ahocorasick;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.search.SearchTerm;

public class SearchState<T> {

    private Node currentNode;
    private final Map<SearchTerm<T>, List<Long>> resultMap;
    private long bytesRead;

    SearchState(final Node rootNode) {
        resultMap = new HashMap<>(5);
        currentNode = rootNode;
        bytesRead = 0L;
    }

    void incrementBytesRead(final long increment) {
        bytesRead += increment;
    }

    void setCurrentNode(final Node curr) {
        currentNode = curr;
    }

    public Node getCurrentNode() {
        return currentNode;
    }

    public Map<SearchTerm<T>, List<Long>> getResults() {
        return new HashMap<>(resultMap);
    }

    void addResult(final SearchTerm matchingTerm) {
        final List<Long> indexes = (resultMap.containsKey(matchingTerm)) ? resultMap.get(matchingTerm) : new ArrayList<Long>(5);
        indexes.add(bytesRead);
        resultMap.put(matchingTerm, indexes);
    }

    public boolean foundMatch() {
        return !resultMap.isEmpty();
    }
}
