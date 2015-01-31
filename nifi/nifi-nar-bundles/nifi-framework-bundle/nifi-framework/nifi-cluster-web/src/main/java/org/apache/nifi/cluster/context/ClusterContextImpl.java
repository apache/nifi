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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.nifi.action.Action;
import org.apache.nifi.web.Revision;

/**
 * A basic implementation of the context.
 */
public class ClusterContextImpl implements ClusterContext, Serializable {

    private final List<Action> actions = new ArrayList<>();
    
    private Revision revision;
    
    private boolean requestSentByClusterManager;
    
    private final String idGenerationSeed = UUID.randomUUID().toString();
    
    @Override
    public List<Action> getActions() {
        return actions;
    }

    @Override
    public Revision getRevision() {
        return revision;
    }

    @Override
    public void setRevision(Revision revision) {
        this.revision = revision;
    }

    @Override
    public boolean isRequestSentByClusterManager() {
        return requestSentByClusterManager;
    }
    
    @Override
    public void setRequestSentByClusterManager(boolean requestSentByClusterManager) {
        this.requestSentByClusterManager = requestSentByClusterManager;
    }

    @Override
    public String getIdGenerationSeed() {
        return this.idGenerationSeed;
    }

}
