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

package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsumerPartitionsUtil {
    public static final String PARTITION_PROPERTY_NAME_PREFIX = "partitions.";

    public static int[] getPartitionsForHost(final Map<String, String> properties, final ComponentLog logger) throws UnknownHostException {
        final Map<String, String> hostnameToPartitionString = mapHostnamesToPartitionStrings(properties);
        final Map<String, int[]> partitionsByHost = mapPartitionValueToIntArrays(hostnameToPartitionString);

        if (partitionsByHost.isEmpty()) {
            // Explicit partitioning is not enabled.
            logger.debug("No explicit Consumer Partitions have been declared.");
            return null;
        }

        logger.info("Found the following mapping of hosts to partitions: {}", new Object[] {hostnameToPartitionString});

        // Determine the partitions based on hostname/IP.
        int[] partitionsForThisHost = getPartitionsForThisHost(partitionsByHost);
        if (partitionsForThisHost == null) {
            throw new IllegalArgumentException("Could not find a partition mapping for host " + InetAddress.getLocalHost().getCanonicalHostName());
        }

        return partitionsForThisHost;
    }

    private static Map<String, int[]> mapPartitionValueToIntArrays(final Map<String, String> partitionValues) {
        final Map<String, int[]> partitionsByHost = new HashMap<>();
        for (final Map.Entry<String, String> entry : partitionValues.entrySet()) {
            final String host = entry.getKey();
            final int[] partitions = parsePartitions(host, entry.getValue());
            partitionsByHost.put(host, partitions);
        }

        return partitionsByHost;
    }

    private static int[] getPartitionsForThisHost(final Map<String, int[]> partitionsByHost) throws UnknownHostException {
        // Determine the partitions based on hostname/IP.
        final InetAddress localhost = InetAddress.getLocalHost();
        int[] partitionsForThisHost = partitionsByHost.get(localhost.getCanonicalHostName());
        if (partitionsForThisHost != null) {
            return partitionsForThisHost;
        }

        partitionsForThisHost = partitionsByHost.get(localhost.getHostName());
        if (partitionsForThisHost != null) {
            return partitionsForThisHost;
        }

        return partitionsByHost.get(localhost.getHostAddress());
    }

    private static Map<String, String> mapHostnamesToPartitionStrings(final Map<String, String> properties) {
        final Map<String, String> hostnameToPartitionString = new HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String propertyName = entry.getKey();
            if (!propertyName.startsWith(PARTITION_PROPERTY_NAME_PREFIX)) {
                continue;
            }

            if (propertyName.length() <= PARTITION_PROPERTY_NAME_PREFIX.length()) {
                continue;
            }

            final String propertyNameAfterPrefix = propertyName.substring(PARTITION_PROPERTY_NAME_PREFIX.length());
            hostnameToPartitionString.put(propertyNameAfterPrefix, entry.getValue());
        }

        return hostnameToPartitionString;
    }

    private static int[] parsePartitions(final String hostname, final String propertyValue) {
        final String[] splits = propertyValue.split(",");
        final List<Integer> partitionList = new ArrayList<>();
        for (final String split : splits) {
            if (split.trim().isEmpty()) {
                continue;
            }

            try {
                final int partition = Integer.parseInt(split.trim());
                if (partition < 0) {
                    throw new IllegalArgumentException("Found invalid value for the partitions for hostname " + hostname + ": " + split + " is negative");
                }

                partitionList.add(partition);
            } catch (final NumberFormatException nfe) {
                throw new IllegalArgumentException("Found invalid value for the partitions for hostname " + hostname + ": " + split + " is not an integer");
            }
        }

        // Map out List<Integer> to int[]
        return partitionList.stream().mapToInt(Integer::intValue).toArray();
    }

    public static ValidationResult validateConsumePartitions(final Map<String, String> properties) {
        final Map<String, String> hostnameToPartitionMapping = mapHostnamesToPartitionStrings(properties);
        if (hostnameToPartitionMapping.isEmpty()) {
            // Partitions are not being explicitly assigned.
            return new ValidationResult.Builder().valid(true).build();
        }

        final Set<Integer> partitionsClaimed = new HashSet<>();
        final Set<Integer> duplicatePartitions = new HashSet<>();
        for (final Map.Entry<String, String> entry : hostnameToPartitionMapping.entrySet()) {
            final int[] partitions = parsePartitions(entry.getKey(), entry.getValue());
            for (final int partition : partitions) {
                final boolean added = partitionsClaimed.add(partition);
                if (!added) {
                    duplicatePartitions.add(partition);
                }
            }
        }

        final List<Integer> partitionsMissing = new ArrayList<>();
        for (int i=0; i < partitionsClaimed.size(); i++) {
            if (!partitionsClaimed.contains(i)) {
                partitionsMissing.add(i);
            }
        }

        if (!partitionsMissing.isEmpty()) {
            return new ValidationResult.Builder()
                .subject("Partitions")
                .input(partitionsClaimed.toString())
                .valid(false)
                .explanation("The following partitions were not mapped to any node: " + partitionsMissing.toString())
                .build();
        }

        if (!duplicatePartitions.isEmpty()) {
            return new ValidationResult.Builder()
                .subject("Partitions")
                .input(partitionsClaimed.toString())
                .valid(false)
                .explanation("The following partitions were mapped to multiple nodes: " + duplicatePartitions.toString())
                .build();
        }

        final Map<String, int[]> partitionsByHost = mapPartitionValueToIntArrays(hostnameToPartitionMapping);
        final int[] partitionsForThisHost;
        try {
            partitionsForThisHost = getPartitionsForThisHost(partitionsByHost);
        } catch (UnknownHostException e) {
            return new ValidationResult.Builder()
                .valid(false)
                .subject("Partition Assignment")
                .explanation("Unable to determine hostname of localhost")
                .build();
        }

        if (partitionsForThisHost == null) {
            return new ValidationResult.Builder()
                .subject("Partition Assignment")
                .valid(false)
                .explanation("No assignment was given for this host")
                .build();
        }

        return new ValidationResult.Builder().valid(true).build();
    }

    public static boolean isPartitionAssignmentExplicit(final Map<String, String> properties) {
        final Map<String, String> hostnameToPartitionMapping = mapHostnamesToPartitionStrings(properties);
        return !hostnameToPartitionMapping.isEmpty();
    }

    public static int getPartitionAssignmentCount(final Map<String, String> properties) {
        final Map<String, String> hostnameToPartitionMapping = mapHostnamesToPartitionStrings(properties);
        final Map<String, int[]> partitions = mapPartitionValueToIntArrays(hostnameToPartitionMapping);

        int count = 0;
        for (final int[] partitionArray : partitions.values()) {
            count += partitionArray.length;
        }

        return count;
    }
}
