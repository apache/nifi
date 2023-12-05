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
package org.apache.nifi.questdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

final class DeleteOldRolloverStrategy implements RolloverStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteOldRolloverStrategy.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final String DELETION_QUERY = "ALTER TABLE %s DROP PARTITION LIST '%s'";
    // Distinct keyword is not recognized if the date mapping is not within an inner query
    private static final String PARTITION_SELECTION_QUERY = "SELECT DISTINCT * FROM (SELECT (to_str(capturedAt, 'yyyy-MM-dd')) AS partitionName FROM %s)";

    private static final RequestMapping<Partition> PARTITION_MAPPING = RequestMappingBuilder.of(Partition::new)
        .addStringField(Partition::setName)
        .build();

    private final Supplier<ZonedDateTime> timeSource;
    private final int daysToKeep;

    DeleteOldRolloverStrategy(final Supplier<ZonedDateTime> timeSource, final int daysToKeep) {
        this.timeSource = timeSource;
        this.daysToKeep = daysToKeep;
    }

    @Override
    public void rollOver(final Client client, final String tableName) {
        try {
            final List<Partition> partitions = getPartitions(client, tableName);
            final String oldestPartitionToKeep = getOldestPartitionToKeep();

            // The last partition if exists, it is considered as "active partition" and cannot be deleted.
            for (int i = 0; i < partitions.size() - 1; i++) {
                final String partition = partitions.get(i).getName();
                if (oldestPartitionToKeep.compareTo(partition) > 0) {
                    try {
                        client.execute(String.format(DELETION_QUERY, tableName, partition));
                        LOGGER.debug("Dropping partition [{}] of table [{}] was successful", partition, tableName);
                    } catch (final Exception e) {
                        LOGGER.error("Dropping partition [{}] of table [{}] failed", partition, tableName, e);
                    }
                }
            }
        } catch (final Exception e2) {
            LOGGER.error("Rollover failed for table [{}]", tableName, e2);
        }
    }

    private List<Partition> getPartitions(final Client client, final CharSequence tableName) throws Exception {
        final List<Partition> result = new ArrayList<>();
        final Iterable<Partition> partitions = client.query(String.format(PARTITION_SELECTION_QUERY, new Object[]{tableName}), QueryResultProcessor.forMapping(PARTITION_MAPPING));
        partitions.forEach(result::add);
        Collections.sort(result);
        return result;
    }

    private String getOldestPartitionToKeep() {
        final ZonedDateTime now = timeSource.get();
        final ZonedDateTime utc = now.minusDays(daysToKeep).withZoneSameInstant(ZoneOffset.UTC);
        return utc.format(DATE_FORMATTER);
    }

    private static class Partition implements Comparable<Partition> {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public int compareTo(final Partition o) {
            return name.compareTo(o.getName());
        }
    }
}
