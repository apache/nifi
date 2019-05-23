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
package org.apache.nifi.stateless.core;

import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.List;
import java.util.Queue;

public interface StatelessComponent {

    void shutdown();

    void enqueueAll(Queue<StatelessFlowFile> list);

    boolean runRecursive(Queue<InMemoryFlowFile> queue);

    boolean validate();

    void addIncomingConnection(String connectionId);

    void addOutputPort(Relationship relationship, boolean isFailurePort);

    boolean isMaterializeContent();

    List<StatelessComponent> getParents();

    void addParent(StatelessComponent parent);

    void addChild(StatelessComponent child, Relationship relationship);
}
