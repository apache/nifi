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
     * @return the unique identifier for this flow file which is guaranteed
     * to be unique within a single running instance of nifi.  This identifier
     * should not be used for true universal unique type needs.  For that consider
     * using the attribute found in the flow file's attribute map keyed by
     * {@link org.apache.nifi.flowfile.attributes.CoreAttributes.UUID CoreAttributes.UUID}.
     * For example, by calling getAttribute(CoreAttributes.UUID.getKey()).
     */
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
     * Returns a 64-bit integer that indicates the order in which the FlowFile was added to the
     * flow with respect to other FlowFiles that have the same last lineage start date.
     * I.e., if two FlowFiles return the same value for {@link #getLineageStartDate()}, the order
     * in which those FlowFiles were added to the flow can be determined by looking at the result of
     * this method. However, no guarantee is made by this method about the ordering of FlowFiles
     * that have different values for the {@link #getLineageStartDate()} method.
     *
     * @return the index that can be used to compare two FlowFiles with the same lineage start date
     * to understand the order in which the two FlowFiles were enqueued.
     */
    long getLineageStartIndex();

    /**
     * @return the time at which the FlowFile was most recently added to a
     * FlowFile queue, or {@code null} if the FlowFile has never been enqueued.
     * This value will always be populated before it is passed to a
     * FlowFilePrioritizer
     */
    Long getLastQueueDate();

    /**
     * Returns a 64-bit integer that indicates the order in which the FlowFile was added to the
     * FlowFile queue with respect to other FlowFiles that have the same last queue date.
     * I.e., if two FlowFiles return the same value for {@link #getLastQueueDate()}, the order
     * in which those FlowFiles were enqueued can be determined by looking at the result of
     * this method. However, no guarantee is made by this method about the ordering of FlowFiles
     * that have different values for the {@link #getLastQueueDate()} method.
     *
     * @return the index that can be used to compare two FlowFiles with the same last queue date
     * to understand the order in which the two FlowFiles were enqueued.
     */
    long getQueueDateIndex();

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
