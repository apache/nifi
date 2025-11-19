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
package org.apache.nifi.web.api.request;


import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

public enum FlowMetricsReportingStrategy implements DescribedValue {
    ALL_PROCESS_GROUPS(new AllowableValue("All Process Groups", "All Process Groups",
            "Send metrics for each process group")),
    ALL_COMPONENTS(new AllowableValue("All Components", "All Components",
            "Send metrics for each component in the system, to include processors, connections, controller services, etc."));

    private final AllowableValue strategy;

    FlowMetricsReportingStrategy(final AllowableValue strategy) {
        this.strategy = strategy;
    }

    @Override
    public String getValue() {
        return this.strategy.getValue();
    }

    @Override
    public String getDisplayName() {
        return this.strategy.getDisplayName();
    }

    @Override
    public String getDescription() {
        return this.strategy.getDescription();
    }
}
