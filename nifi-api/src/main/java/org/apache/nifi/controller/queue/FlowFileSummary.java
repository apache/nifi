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

package org.apache.nifi.controller.queue;

/**
 * A summary of a FlowFile that can be used to represent a "high level" view of FlowFile
 * without providing all of the information available.
 */
public interface FlowFileSummary {
    /**
     * @return the UUID of the FlowFile
     */
    String getUuid();

    /**
     * @return the value of the 'filename' attribute
     */
    String getFilename();

    /**
     * @return the current position of the FlowFile in the queue based on the prioritizers selected
     */
    int getPosition();

    /**
     * @return the size of the FlowFile in bytes
     */
    long getSize();

    /**
     * @return the timestamp (in milliseconds since epoch) at which the FlowFile was added to the queue
     */
    long getLastQueuedTime();

    /**
     * @return the timestamp (in milliseconds since epoch) at which the FlowFile's greatest ancestor entered the flow
     */
    long getLineageStartDate();

    /**
     * @return <code>true</code> if the FlowFile is penalized, <code>false</code> otherwise
     */
    boolean isPenalized();
}
