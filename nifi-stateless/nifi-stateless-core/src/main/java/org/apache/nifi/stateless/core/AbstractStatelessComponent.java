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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractStatelessComponent implements StatelessComponent {
    private List<StatelessComponent> parents = new ArrayList<>();
    private List<String> incomingConnections = new ArrayList<>();
    private final Map<Relationship, List<StatelessComponent>> children = new HashMap<>();
    private final Set<Relationship> autoTermination = new HashSet<>();
    private final Set<Relationship> successOutputPorts = new HashSet<>();
    private final Set<Relationship> failureOutputPorts = new HashSet<>();


    public AbstractStatelessComponent() {

    }

    public List<StatelessComponent> getParents() {
        return Collections.unmodifiableList(parents);
    }

    public void addParent(final StatelessComponent parent) {
        if (parent != null) {
            parents.add(parent);
        }
    }

    public void addIncomingConnection(final String connectionId) {
        this.incomingConnections.add(connectionId);
    }

    public void addOutputPort(Relationship relationship, boolean isFailurePort) {
        if (isFailurePort) {
            this.failureOutputPorts.add(relationship);
        } else {
            this.successOutputPorts.add(relationship);
        }
    }

    public void addChild(StatelessComponent child, Relationship relationship) {
        List<StatelessComponent> list = children.computeIfAbsent(relationship, r -> new ArrayList<>());
        list.add(child);

        getContext().addConnection(relationship);
    }

    public void addAutoTermination(Relationship relationship) {
        this.autoTermination.add(relationship);
        getContext().addConnection(relationship);
    }


    public boolean validate() {
        if (!getContext().isValid()) {
            return false;
        }

        for (final Relationship relationship : getRelationships()) {
            boolean hasChildren = this.children.containsKey(relationship);
            boolean hasAutoterminate = this.autoTermination.contains(relationship);
            boolean hasFailureOutputPort = this.failureOutputPorts.contains(relationship);
            boolean hasSuccessOutputPort = this.successOutputPorts.contains(relationship);

            if (!(hasChildren || hasAutoterminate || hasFailureOutputPort || hasSuccessOutputPort)) {
                getLogger().error("Component: {}, Relationship: {}, needs either auto terminate, child processors, or an output port", new Object[] {toString(), relationship.getName()});
                return false;
            }
        }

        for (final Map.Entry<Relationship, List<StatelessComponent>> entry : this.children.entrySet()) {
            for (final StatelessComponent component : entry.getValue()) {
                if (!component.validate()) {
                    return false;
                }
            }
        }

        return true;
    }

    protected Map<Relationship, List<StatelessComponent>> getChildren() {
        return children;
    }

    protected Set<Relationship> getSuccessOutputPorts() {
        return successOutputPorts;
    }

    protected Set<Relationship> getFailureOutputPorts() {
        return failureOutputPorts;
    }

    protected boolean isAutoTerminated(final Relationship relationship) {
        return autoTermination.contains(relationship);
    }



    public abstract Set<Relationship> getRelationships();

    protected abstract StatelessConnectionContext getContext();

    protected abstract ComponentLog getLogger();
}
