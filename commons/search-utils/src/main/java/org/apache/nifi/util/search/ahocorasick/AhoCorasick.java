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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.nifi.util.search.Search;
import org.apache.nifi.util.search.SearchTerm;

public class AhoCorasick<T> implements Search<T> {

    private Node root = null;

    /**
     * Constructs a new search object.
     *
     * @throws IllegalArgumentException if given terms are null or empty
     */
    public AhoCorasick() {
    }

    @Override
    public void initializeDictionary(final Set<SearchTerm<T>> terms) {
        if (root != null) {
            throw new IllegalStateException();
        }
        root = new Node();
        if (terms == null || terms.isEmpty()) {
            throw new IllegalArgumentException();
        }
        for (final SearchTerm<T> term : terms) {
            int i = 0;
            Node nextNode = root;
            while (true) {
                nextNode = addMatch(term, i, nextNode);
                if (nextNode == null) {
                    break; //we're done
                }
                i++;
            }
        }
        initialize();
    }

    private Node addMatch(final SearchTerm<T> term, final int offset, final Node current) {
        final int index = term.get(offset);
        boolean atEnd = (offset == (term.size() - 1));
        if (current.getNeighbor(index) == null) {
            if (atEnd) {
                current.setNeighbor(new Node(term), index);
                return null;
            }
            current.setNeighbor(new Node(), index);
        } else if (atEnd) {
            current.getNeighbor(index).setMatchingTerm(term);
            return null;
        }
        return current.getNeighbor(index);
    }

    private void initialize() {
        //perform bgs to build failure links
        final Queue<Node> queue = new LinkedList<>();
        queue.add(root);
        root.setFailureNode(null);
        while (!queue.isEmpty()) {
            final Node current = queue.poll();
            for (int i = 0; i < 256; i++) {
                final Node next = current.getNeighbor(i);
                if (next != null) {
                    //traverse failure to get state
                    Node fail = current.getFailureNode();
                    while ((fail != null) && fail.getNeighbor(i) == null) {
                        fail = fail.getFailureNode();
                    }
                    if (fail != null) {
                        next.setFailureNode(fail.getNeighbor(i));
                    } else {
                        next.setFailureNode(root);
                    }
                    queue.add(next);
                }
            }
        }
    }

    @Override
    public SearchState search(final InputStream stream, final boolean findAll) throws IOException {
        return search(stream, findAll, null);
    }

    private SearchState search(final InputStream stream, final boolean findAll, final SearchState state) throws IOException {
        if (root == null) {
            throw new IllegalStateException();
        }
        final SearchState<T> currentState = (state == null) ? new SearchState(root) : state;
        if (!findAll && currentState.foundMatch()) {
            throw new IllegalStateException("A match has already been found yet we're being asked to keep searching");
        }
        Node current = currentState.getCurrentNode();
        int currentChar;
        while ((currentChar = stream.read()) >= 0) {
            currentState.incrementBytesRead(1L);
            Node next = current.getNeighbor(currentChar);
            if (next == null) {
                next = current.getFailureNode();
                while ((next != null) && next.getNeighbor(currentChar) == null) {
                    next = next.getFailureNode();
                }
                if (next != null) {
                    next = next.getNeighbor(currentChar);
                } else {
                    next = root;
                }
            }
            if (next == null) {
                throw new IllegalStateException("tree out of sync");
            }
            //Accept condition
            if (next.hasMatch()) {
                currentState.addResult(next.getMatchingTerm());
            }
            for (Node failNode = next.getFailureNode(); failNode != null; failNode = failNode.getFailureNode()) {
                if (failNode.hasMatch()) {
                    currentState.addResult(failNode.getMatchingTerm());
                }
            }
            current = next;
            if (currentState.foundMatch() && !findAll) {
                break;//give up as soon as we have at least one match
            }
        }
        currentState.setCurrentNode(current);
        return currentState;
    }

}
