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
 * The context that will be passed to the flow provider when saving a snapshot of a versioned flow.
 */
public interface FlowSnapshotContext {

    /**
     * @return the id of the bucket this snapshot belongs to
     */
    String getBucketId();

    /**
     * @return the name of the bucket this snapshot belongs to
     */
    String getBucketName();

    /**
     * @return the id of the versioned flow this snapshot belongs to
     */
    String getFlowId();

    /**
     * @return the name of the versioned flow this snapshot belongs to
     */
    String getFlowName();

    /**
     * @return the description of the flow this snapshot belongs to
     */
    String getFlowDescription();

    /**
     * @return the version of the snapshot
     */
    int getVersion();

    /**
     * @return the comments for the snapshot
     */
    String getComments();

    /**
     * @return the timestamp the snapshot was created
     */
    long getSnapshotTimestamp();

    /**
     * @return the author of the snapshot
     */
    String getAuthor();

}
