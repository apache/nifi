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
package org.apache.nifi.util;

import java.util.Collection;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceReporter;

public class MockProvenanceReporter implements ProvenanceReporter {

    @Override
    public void receive(FlowFile flowFile, String sourceSystemUri) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, boolean force) {

    }

    @Override
    public void receive(FlowFile flowFile, String sourceSystemUri, long transmissionMillis) {

    }

    @Override
    public void receive(FlowFile flowFile, String sourceSystemUri, String sourceSystemFlowFileIdentifier) {

    }

    @Override
    public void receive(FlowFile flowFile, String sourceSystemUri, String sourceSystemFlowFileIdentifier, long transmissionMillis) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, long transmissionMillis) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, long transmissionMillis, boolean force) {

    }

    @Override
    public void associate(FlowFile flowFile, String alternateIdentifierNamespace, String alternateIdentifier) {

    }

    @Override
    public void fork(FlowFile parent, Collection<FlowFile> children) {

    }

    @Override
    public void fork(FlowFile parent, Collection<FlowFile> children, long forkDuration) {

    }

    @Override
    public void fork(FlowFile parent, Collection<FlowFile> children, String details) {

    }

    @Override
    public void fork(FlowFile parent, java.util.Collection<FlowFile> children, String details, long forkDuration) {

    }

    @Override
    public void join(Collection<FlowFile> parents, FlowFile child) {

    }

    @Override
    public void join(Collection<FlowFile> parents, FlowFile child, long joinDuration) {

    }

    @Override
    public void join(Collection<FlowFile> parents, FlowFile child, String details) {

    }

    @Override
    public void join(java.util.Collection<FlowFile> parents, FlowFile child, String details, long joinDuration) {

    }

    @Override
    public void clone(FlowFile parent, FlowFile child) {

    }

    @Override
    public void modifyContent(FlowFile flowFile) {

    }

    @Override
    public void modifyContent(FlowFile flowFile, String details) {

    }

    @Override
    public void modifyContent(FlowFile flowFile, long processingMillis) {

    }

    @Override
    public void modifyContent(FlowFile flowFile, String details, long processingMillis) {

    }

    @Override
    public void modifyAttributes(FlowFile flowFile) {

    }

    @Override
    public void modifyAttributes(FlowFile flowFile, String details) {

    }

    @Override
    public void route(FlowFile flowFile, Relationship relationship) {

    }

    @Override
    public void route(FlowFile flowFile, Relationship relationship, String details) {

    }

    @Override
    public void route(FlowFile flowFile, Relationship relationship, long processingDuration) {

    }

    @Override
    public void route(FlowFile flowFile, Relationship relationship, String details, long processingDuration) {

    }

    @Override
    public void create(FlowFile flowFile) {

    }

    @Override
    public void create(FlowFile flowFile, String details) {

    }

    @Override
    public void receive(FlowFile flowFile, String sourceSystemUri, String sourceSystemFlowFileIdentifier, String details, long transmissionMillis) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, String details) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, String details, long transmissionMillis) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, String details, boolean force) {

    }

    @Override
    public void send(FlowFile flowFile, String destinationSystemUri, String details, long transmissionMillis, boolean force) {

    }

}
