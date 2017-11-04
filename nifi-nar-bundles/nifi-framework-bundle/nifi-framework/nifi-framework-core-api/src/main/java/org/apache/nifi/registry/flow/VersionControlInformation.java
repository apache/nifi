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

package org.apache.nifi.registry.flow;

/**
 * <p>
 * Provides a mechanism for conveying which Flow Registry a flow is stored in, and
 * where in the Flow Registry the flow is stored.
 * </p>
 */
public interface VersionControlInformation {

    /**
     * @return the unique identifier of the Flow Registry that this flow is tracking to
     */
    String getRegistryIdentifier();

    /**
     * @return the name of the Flow Registry that this flow is tracking to
     */
    String getRegistryName();

    /**
     * @return the unique identifier of the bucket that this flow belongs to
     */
    String getBucketIdentifier();

    /**
     * @return the name of the bucket that this flow belongs to
     */
    String getBucketName();

    /**
     * @return the unique identifier of this flow in the Flow Registry
     */
    String getFlowIdentifier();

    /**
     * @return the name of the flow
     */
    String getFlowName();

    /**
     * @return the description of the flow
     */
    String getFlowDescription();

    /**
     * @return the version of the flow in the Flow Registry that this flow is based on.
     */
    int getVersion();

    /**
     * @return <code>true</code> if the flow has been modified since the last time that it was updated from the Flow Registry or saved
     *         to the Flow Registry; <code>false</code> if the flow is in sync with the Flow Registry.
     */
    boolean isModified();

    /**
     * @return <code>true</code> if this version of the flow is the most recent version of the flow available in the Flow Registry, <code>false</code> otherwise.
     */
    boolean isCurrent();

    /**
     * @return the snapshot of the flow that was synchronized with the Flow Registry
     */
    VersionedProcessGroup getFlowSnapshot();
}
