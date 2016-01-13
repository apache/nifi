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
package org.apache.nifi.processors.hadoop.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.fs.Path;

/**
 * A simple POJO for maintaining state about the last HDFS Listing that was performed so that
 * we can avoid pulling the same file multiple times
 */
@XmlType(name = "listing")
public class HDFSListing {
    private Date latestTimestamp;
    private Collection<String> matchingPaths;

    public static class StateKeys {
        public static final String TIMESTAMP = "timestamp";
        public static final String PATH_PREFIX = "path.";
    }

    /**
     * @return the modification date of the newest file that was contained in the HDFS Listing
     */
    public Date getLatestTimestamp() {
        return latestTimestamp;
    }

    /**
     * Sets the timestamp of the modification date of the newest file that was contained in the HDFS Listing
     *
     * @param latestTimestamp the timestamp of the modification date of the newest file that was contained in the HDFS Listing
     */
    public void setLatestTimestamp(Date latestTimestamp) {
        this.latestTimestamp = latestTimestamp;
    }

    /**
     * @return a Collection containing the paths of all files in the HDFS Listing whose Modification date
     * was equal to {@link #getLatestTimestamp()}
     */
    @XmlTransient
    public Collection<String> getMatchingPaths() {
        return matchingPaths;
    }

    /**
     * @return a Collection of {@link Path} objects equivalent to those returned by {@link #getMatchingPaths()}
     */
    public Set<Path> toPaths() {
        final Set<Path> paths = new HashSet<>(matchingPaths.size());
        for ( final String pathname : matchingPaths ) {
            paths.add(new Path(pathname));
        }
        return paths;
    }

    /**
     * Sets the Collection containing the paths of all files in the HDFS Listing whose Modification Date was
     * equal to {@link #getLatestTimestamp()}
     * @param matchingPaths the paths that have last modified date matching the latest timestamp
     */
    public void setMatchingPaths(Collection<String> matchingPaths) {
        this.matchingPaths = matchingPaths;
    }

    /**
     * Converts this HDFSListing into a Map&lt;String, String&gt; so that it can be stored in a StateManager.
     *
     * @return a Map that represents the same information as this HDFSListing
     */
    public Map<String, String> toMap() {
        final Map<String, String> map = new HashMap<>(1 + matchingPaths.size());
        map.put(StateKeys.TIMESTAMP, String.valueOf(latestTimestamp.getTime()));

        int counter = 0;
        for (final String path : matchingPaths) {
            map.put(StateKeys.PATH_PREFIX + String.valueOf(counter++), path);
        }

        return map;
    }

    public static HDFSListing fromMap(final Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        final String timestampValue = map.get(StateKeys.TIMESTAMP);
        final long timestamp = Long.parseLong(timestampValue);

        final Collection<String> matchingPaths = new ArrayList<>(map.size() - 1);
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(StateKeys.PATH_PREFIX)) {
                matchingPaths.add(entry.getValue());
            }
        }

        final HDFSListing listing = new HDFSListing();
        listing.setLatestTimestamp(new Date(timestamp));
        listing.setMatchingPaths(matchingPaths);
        return listing;
    }
}
