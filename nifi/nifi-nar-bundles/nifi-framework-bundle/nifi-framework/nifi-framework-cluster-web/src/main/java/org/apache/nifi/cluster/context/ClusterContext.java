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
package org.apache.nifi.cluster.context;

import java.io.Serializable;
import java.util.List;
import org.apache.nifi.action.Action;
import org.apache.nifi.web.Revision;

/**
 * Contains contextual information about clustering that may be serialized
 * between manager and node when communicating over HTTP.
 */
public interface ClusterContext extends Serializable {

    /**
     * Returns a list of auditable actions. The list is modifiable and will
     * never be null.
     *
     * @return a collection of actions
     */
    List<Action> getActions();

    Revision getRevision();

    void setRevision(Revision revision);

    /**
     * @return true if the request was sent by the cluster manager; false
     * otherwise
     */
    boolean isRequestSentByClusterManager();

    /**
     * Sets the flag to indicate if a request was sent by the cluster manager.
     *
     * @param flag true if the request was sent by the cluster manager; false
     * otherwise
     */
    void setRequestSentByClusterManager(boolean flag);

    /**
     * Gets an id generation seed. This is used to ensure that nodes are able to
     * generate the same id across the cluster. This is usually handled by the
     * cluster manager creating the id, however for some actions (snippets,
     * templates, etc) this is not possible.
     *
     * @return generated id seed
     */
    String getIdGenerationSeed();
}
