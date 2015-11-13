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
package org.apache.nifi.flowfile;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 * A flow file is a logical notion of an item in a flow with its associated
 * attributes and identity which can be used as a reference for its actual
 * content.</p>
 *
 * <b>All FlowFile implementations must be Immutable - Thread safe.</b>
 */
public interface FlowFile extends Comparable<FlowFile> {

    /**
     * @return the unique identifier for this flow file
     * @deprecated This method has been deprecated in favor of using the attribute
     *             {@link org.apache.nifi.flowfile.attributes.CoreAttributes.UUID CoreAttributes.UUID}.
     *             If an identifier is needed use {@link #getAttribute(String)} to retrieve the value for this attribute.
     *             For example, by calling getAttribute(CoreAttributes.UUID.getKey()).
     */
    @Deprecated
    long getId();

    /**
     * @return the date at which the flow file entered the flow
     */
    long getEntryDate();

    /**
     * @return the date at which the origin of this FlowFile entered the flow.
     * For example, if FlowFile Z were derived from FlowFile Y and FlowFile Y
     * was derived from FlowFile X, this date would be the entryDate (see
     * {@link #getEntryDate()} of FlowFile X.
     */
    long getLineageStartDate();

    /**
     * @return the time at which the FlowFile was most recently added to a
     * FlowFile queue, or {@code null} if the FlowFile has never been enqueued.
     * This value will always be populated before it is passed to a
     * {@link FlowFilePrioritizer}
     */
    Long getLastQueueDate();

    /**
     * <p>
     * If a FlowFile is derived from multiple "parent" FlowFiles, all of the
     * parents' Lineage Identifiers will be in the set.
     * </p>
     *
     * @return a set of identifiers that are unique to this FlowFile's lineage.
     * If FlowFile X is derived from FlowFile Y, both FlowFiles will have the
     * same value for the Lineage Claim ID.
     *
     * @deprecated this collection was erroneously unbounded and caused a lot of OutOfMemoryError problems
     *             when dealing with FlowFiles with many ancestors. This Collection is
     *             now capped at 100 lineage identifiers. This method was introduced with the idea of providing
     *             future performance improvements but due to the high cost of heap consumption will not be used
     *             in such a manner. As a result, this method will be removed in a future release.
     */
    @Deprecated
    Set<String> getLineageIdentifiers();

    /**
     * @return true if flow file is currently penalized; false otherwise;
     */
    boolean isPenalized();

    /**
     * Obtains the attribute value for the given key
     *
     * @param key of the attribute
     * @return value if found; null otherwise
     */
    String getAttribute(String key);

    /**
     * @return size of flow file contents in bytes
     */
    long getSize();

    /**
     * @return an unmodifiable map of the flow file attributes
     */
    Map<String, String> getAttributes();

    public static class KeyValidator {

        public static String validateKey(final String key) {
            // We used to validate the key by disallowing a handful of keywords, but this requirement no longer exists.
            // Therefore this method simply verifies that the key is not empty.
            if (key == null) {
                throw new IllegalArgumentException("Invalid attribute key: null");
            }
            if (key.trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid attribute key: <Empty String>");
            }
            return key;
        }
    }
}
