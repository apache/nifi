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

package org.apache.nifi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.IncompleteSwapFileException;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.swap.StandardSwapContents;
import org.apache.nifi.controller.swap.StandardSwapSummary;

public class MockSwapManager implements FlowFileSwapManager {
    public final Map<String, List<FlowFileRecord>> swappedOut = new HashMap<>();
    public int swapOutCalledCount = 0;
    public int swapInCalledCount = 0;

    public int incompleteSwapFileRecordsToInclude = -1;

    public int failSwapInAfterN = -1;
    public Throwable failSwapInFailure = null;

    public void setSwapInFailure(final Throwable t) {
        this.failSwapInFailure = t;
    }

    @Override
    public void initialize(final SwapManagerInitializationContext initializationContext) {

    }

    public void enableIncompleteSwapFileException(final int flowFilesToInclude) {
        incompleteSwapFileRecordsToInclude = flowFilesToInclude;
    }

    @Override
    public String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue, final String partitionName) throws IOException {
        swapOutCalledCount++;
        final String location = UUID.randomUUID().toString() + "." + partitionName;
        swappedOut.put(location, new ArrayList<>(flowFiles));
        return location;
    }

    private void throwIncompleteIfNecessary(final String swapLocation, final boolean remove) throws IOException {
        if (incompleteSwapFileRecordsToInclude > -1) {
            final SwapSummary summary = getSwapSummary(swapLocation);

            final List<FlowFileRecord> records;
            if (remove) {
                records = swappedOut.remove(swapLocation);
            } else {
                records = swappedOut.get(swapLocation);
            }

            final List<FlowFileRecord> partial = records.subList(0, incompleteSwapFileRecordsToInclude);
            final SwapContents partialContents = new StandardSwapContents(summary, partial);
            throw new IncompleteSwapFileException(swapLocation, partialContents);
        }

        if (swapInCalledCount > failSwapInAfterN && failSwapInAfterN > -1) {
            if (failSwapInFailure instanceof RuntimeException) {
                throw (RuntimeException) failSwapInFailure;
            }
            if (failSwapInFailure instanceof Error) {
                throw (Error) failSwapInFailure;
            }

            throw new RuntimeException(failSwapInFailure);
        }
    }

    @Override
    public SwapContents peek(String swapLocation, final FlowFileQueue flowFileQueue) throws IOException {
        throwIncompleteIfNecessary(swapLocation, false);
        return new StandardSwapContents(getSwapSummary(swapLocation), swappedOut.get(swapLocation));
    }

    @Override
    public SwapContents swapIn(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
        swapInCalledCount++;
        throwIncompleteIfNecessary(swapLocation, true);
        return new StandardSwapContents(getSwapSummary(swapLocation), swappedOut.remove(swapLocation));
    }

    @Override
    public List<String> recoverSwapLocations(FlowFileQueue flowFileQueue, final String partitionName) throws IOException {
        return swappedOut.keySet().stream()
            .filter(key -> key.endsWith("." + partitionName))
            .collect(Collectors.toList());
    }

    @Override
    public SwapSummary getSwapSummary(String swapLocation) throws IOException {
        final List<FlowFileRecord> flowFiles = swappedOut.get(swapLocation);
        if (flowFiles == null) {
            return StandardSwapSummary.EMPTY_SUMMARY;
        }

        int count = 0;
        long size = 0L;
        Long max = null;
        final List<ResourceClaim> resourceClaims = new ArrayList<>();
        for (final FlowFileRecord flowFile : flowFiles) {
            count++;
            size += flowFile.getSize();
            if (max == null || flowFile.getId() > max) {
                max = flowFile.getId();
            }

            if (flowFile.getContentClaim() != null) {
                resourceClaims.add(flowFile.getContentClaim().getResourceClaim());
            }
        }

        return new StandardSwapSummary(new QueueSize(count, size), max, resourceClaims);
    }

    @Override
    public void purge() {
        swappedOut.clear();
    }

    @Override
    public Set<String> getSwappedPartitionNames(final FlowFileQueue queue) throws IOException {
        return swappedOut.keySet().stream()
            .filter(key -> key.contains("."))
            .map(key -> key.substring(key.indexOf(".") + 1))
            .collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    public String changePartitionName(final String swapLocation, final String newPartitionName) throws IOException {
        final List<FlowFileRecord> flowFiles = swappedOut.remove(swapLocation);
        if (flowFiles == null) {
            throw new IOException("Could not find swapfile with name " + swapLocation);
        }

        final String newSwapLocation;
        final int dotIndex = swapLocation.indexOf(".");
        if (dotIndex < 0) {
            newSwapLocation = swapLocation + "." + newPartitionName;
        } else {
            newSwapLocation = swapLocation.substring(0, dotIndex) + "." + newPartitionName;
        }

        swappedOut.put(newSwapLocation, flowFiles);
        return newSwapLocation;
    }
}
