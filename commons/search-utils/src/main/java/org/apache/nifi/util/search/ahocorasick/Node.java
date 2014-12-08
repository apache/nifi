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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.search.SearchTerm;

/**
 *
 * @author
 */
public class Node {

    private final Map<Integer, Node> neighborMap;
    private Node failureNode;
    private SearchTerm<?> term;

    Node(final SearchTerm<?> term) {
        this();
        this.term = term;
    }

    Node() {
        neighborMap = new HashMap<>();
        term = null;
    }

    void setFailureNode(final Node fail) {
        failureNode = fail;
    }

    public Node getFailureNode() {
        return failureNode;
    }

    public boolean hasMatch() {
        return term != null;
    }

    void setMatchingTerm(final SearchTerm<?> term) {
        this.term = term;
    }

    public SearchTerm<?> getMatchingTerm() {
        return term;
    }

    public Node getNeighbor(final int index) {
        return neighborMap.get(index);
    }

    void setNeighbor(final Node neighbor, final int index) {
        neighborMap.put(index, neighbor);
    }

}
