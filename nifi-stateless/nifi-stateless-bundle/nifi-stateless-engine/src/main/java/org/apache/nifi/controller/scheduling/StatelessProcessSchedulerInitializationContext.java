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

package org.apache.nifi.controller.scheduling;

import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.stateless.engine.ProcessContextFactory;

public class StatelessProcessSchedulerInitializationContext {
    private final ProcessContextFactory processContextFactory;
    private final FlowEngine componentLifeCycleThreadPool;
    private final FlowEngine componentMonitoringThreadPool;
    private final FlowEngine frameworkTaskThreadPool;
    private final boolean manageThreadPools;

    private StatelessProcessSchedulerInitializationContext(final Builder builder) {
        this.processContextFactory = builder.processContextFactory;
        this.componentLifeCycleThreadPool = builder.componentLifeCycleThreadPool;
        this.componentMonitoringThreadPool = builder.componentMonitoringThreadPool;
        this.frameworkTaskThreadPool = builder.frameworkTaskThreadPool;
        this.manageThreadPools = builder.manageThreadPools;
    }

    public ProcessContextFactory getProcessContextFactory() {
        return processContextFactory;
    }

    public FlowEngine getComponentLifeCycleThreadPool() {
        return componentLifeCycleThreadPool;
    }

    public FlowEngine getComponentMonitoringThreadPool() {
        return componentMonitoringThreadPool;
    }

    public FlowEngine getFrameworkTaskThreadPool() {
        return frameworkTaskThreadPool;
    }

    public boolean isManageThreadPools() {
        return manageThreadPools;
    }

    public static final class Builder {
        private ProcessContextFactory processContextFactory;
        private FlowEngine componentLifeCycleThreadPool;
        private FlowEngine componentMonitoringThreadPool;
        private FlowEngine frameworkTaskThreadPool;
        private boolean manageThreadPools;

        public Builder processContextFactory(final ProcessContextFactory processContextFactory) {
            this.processContextFactory = processContextFactory;
            return this;
        }

        public Builder componentLifeCycleThreadPool(final FlowEngine componentLifeCycleThreadPool) {
            this.componentLifeCycleThreadPool = componentLifeCycleThreadPool;
            return this;
        }

        public Builder componentMonitoringThreadPool(final FlowEngine componentMonitoringThreadPool) {
            this.componentMonitoringThreadPool = componentMonitoringThreadPool;
            return this;
        }

        public Builder frameworkTaskThreadPool(final FlowEngine frameworkTaskThreadPool) {
            this.frameworkTaskThreadPool = frameworkTaskThreadPool;
            return this;
        }

        public Builder manageThreadPools(final boolean manageThreadPools) {
            this.manageThreadPools = manageThreadPools;
            return this;
        }

        public StatelessProcessSchedulerInitializationContext build() {
            return new StatelessProcessSchedulerInitializationContext(this);
        }
    }
}
