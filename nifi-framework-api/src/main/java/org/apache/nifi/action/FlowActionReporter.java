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
package org.apache.nifi.action;

import java.io.Closeable;
import java.util.Collection;

/**
 * A {@link FlowActionReporter} is responsible for reporting {@link FlowAction}s.
 * Provides {@link FlowActionReporterConfigurationContext} to allow for configuration and initialization.
 * Uses {@link #close()} to allow for cleanup.
 */
public interface FlowActionReporter extends Closeable {
    /**
     * Initialization method for the reporter. This method is called just after reporter is created.
     * @param context reporter configuration context
     * @throws FlowActionReporterCreationException if case of any error during initialization
     */
    default void onConfigured(FlowActionReporterConfigurationContext context) throws FlowActionReporterCreationException {
    }

    /**
     * Reports a collection of {@link FlowAction}s.
     * @param actions the collection of {@link FlowAction}s to report
     */
    void reportFlowActions(Collection<FlowAction> actions);

    /**
     * Close method for releasing resources when shutting down the application. This method is called just before the reporter is destroyed.
     */
    @Override
    default void close() {
    }
}
