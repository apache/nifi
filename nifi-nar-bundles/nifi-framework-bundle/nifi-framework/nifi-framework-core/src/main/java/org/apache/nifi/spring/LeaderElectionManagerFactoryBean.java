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

package org.apache.nifi.spring;

import org.apache.nifi.controller.leader.election.CuratorLeaderElectionManager;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.StandaloneLeaderElectionManager;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

public class LeaderElectionManagerFactoryBean implements FactoryBean<LeaderElectionManager> {
    private int numThreads;
    private NiFiProperties properties;

    @Override
    public LeaderElectionManager getObject() throws Exception {
        final boolean isNode = properties.isNode();
        if (isNode) {
            return new CuratorLeaderElectionManager(numThreads, properties);
        } else {
            return new StandaloneLeaderElectionManager();
        }
    }

    @Override
    public Class<?> getObjectType() {
        return LeaderElectionManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setNumThreads(final int numThreads) {
        this.numThreads = numThreads;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }
}
