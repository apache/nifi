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
package org.apache.nifi.provenance.lineage;

public interface LineageNode {

    /**
     * Returns the identifier of the Clustered NiFi Node that generated the
     * event
     *
     * @return
     */
    String getClusterNodeIdentifier();

    /**
     * Returns the type of the LineageNode
     *
     * @return
     */
    LineageNodeType getNodeType();

    /**
     * Returns the UUID of the FlowFile for which this Node was created
     *
     * @return
     */
    String getFlowFileUuid();

    /**
     * Returns the UUID for this LineageNode.
     *
     * @return
     */
    String getIdentifier();

    /**
     * Returns the timestamp that corresponds to this Node. The meaning of the
     * timestamp may differ between implementations. For example, a
     * {@link ProvenanceEventLineageNode}'s timestamp indicates the time at
     * which the event occurred. However, for a Node that reperesents a
     * FlowFile, for example, the timestamp may represent the time at which the
     * FlowFile was created.
     *
     * @return
     */
    long getTimestamp();
}
