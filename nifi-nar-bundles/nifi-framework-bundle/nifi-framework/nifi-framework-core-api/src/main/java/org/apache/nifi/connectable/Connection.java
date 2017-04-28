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
package org.apache.nifi.connectable;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.Relationship;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface Connection extends Authorizable {

    void enqueue(FlowFileRecord flowFile);

    void enqueue(Collection<FlowFileRecord> flowFiles);

    Connectable getDestination();

    Authorizable getDestinationAuthorizable();

    Collection<Relationship> getRelationships();

    FlowFileQueue getFlowFileQueue();

    String getIdentifier();

    String getName();

    void setName(String name);

    void setBendPoints(List<Position> position);

    List<Position> getBendPoints();

    int getLabelIndex();

    void setLabelIndex(int labelIndex);

    long getZIndex();

    void setZIndex(long zIndex);

    Connectable getSource();

    Authorizable getSourceAuthorizable();

    void setRelationships(Collection<Relationship> newRelationships);

    void setDestination(final Connectable newDestination);

    void setProcessGroup(ProcessGroup processGroup);

    ProcessGroup getProcessGroup();

    void lock();

    void unlock();

    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    FlowFileRecord poll(Set<FlowFileRecord> expiredRecords);

    void verifyCanUpdate() throws IllegalStateException;

    void verifyCanDelete() throws IllegalStateException;
}
