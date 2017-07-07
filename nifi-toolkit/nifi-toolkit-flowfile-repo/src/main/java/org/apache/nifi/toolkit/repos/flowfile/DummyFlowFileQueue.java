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

package org.apache.nifi.toolkit.repos.flowfile;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;

public class DummyFlowFileQueue implements FlowFileQueue {
    private final String identifier;

    public DummyFlowFileQueue(final String id) {
        this.identifier = id;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public List<FlowFilePrioritizer> getPriorities() {
        return null;
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        return null;
    }

    @Override
    public void purgeSwapFiles() {

    }

    @Override
    public void setPriorities(List<FlowFilePrioritizer> newPriorities) {

    }

    @Override
    public void setBackPressureObjectThreshold(long maxQueueSize) {

    }

    @Override
    public long getBackPressureObjectThreshold() {
        return 0;
    }

    @Override
    public void setBackPressureDataSizeThreshold(String maxDataSize) {

    }

    @Override
    public String getBackPressureDataSizeThreshold() {
        return null;
    }

    @Override
    public QueueSize size() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isActiveQueueEmpty() {
        return false;
    }

    @Override
    public QueueSize getUnacknowledgedQueueSize() {
        return null;
    }

    @Override
    public void acknowledge(FlowFileRecord flowFile) {

    }

    @Override
    public void acknowledge(Collection<FlowFileRecord> flowFiles) {

    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public void put(FlowFileRecord file) {

    }

    @Override
    public void putAll(Collection<FlowFileRecord> files) {

    }

    @Override
    public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords) {
        return null;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords) {
        return null;
    }

    @Override
    public long drainQueue(Queue<FlowFileRecord> sourceQueue, List<FlowFileRecord> destination, int maxResults, Set<FlowFileRecord> expiredRecords) {
        return 0;
    }

    @Override
    public List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords) {
        return null;
    }

    @Override
    public String getFlowFileExpiration() {
        return null;
    }

    @Override
    public int getFlowFileExpiration(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public void setFlowFileExpiration(String flowExpirationPeriod) {

    }

    @Override
    public DropFlowFileStatus dropFlowFiles(String requestIdentifier, String requestor) {
        return null;
    }

    @Override
    public DropFlowFileStatus getDropFlowFileStatus(String requestIdentifier) {
        return null;
    }

    @Override
    public DropFlowFileStatus cancelDropFlowFileRequest(String requestIdentifier) {
        return null;
    }

    @Override
    public ListFlowFileStatus listFlowFiles(String requestIdentifier, int maxResults) {
        return null;
    }

    @Override
    public ListFlowFileStatus getListFlowFileStatus(String requestIdentifier) {
        return null;
    }

    @Override
    public ListFlowFileStatus cancelListFlowFileRequest(String requestIdentifier) {
        return null;
    }

    @Override
    public FlowFileRecord getFlowFile(String flowFileUuid) throws IOException {
        return null;
    }

    @Override
    public void verifyCanList() throws IllegalStateException {

    }

}
