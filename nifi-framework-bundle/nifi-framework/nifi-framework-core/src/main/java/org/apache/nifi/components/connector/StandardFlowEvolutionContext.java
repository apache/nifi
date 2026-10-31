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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.Bundle;

import java.util.concurrent.atomic.AtomicBoolean;

public class StandardFlowEvolutionContext implements FlowEvolutionContext {
    private static final String CLOSED_MESSAGE = "Flow evolution has failed and the context can no longer be used";

    private final FlowContext preUpgradeActiveFlowContext;
    private final Bundle postUpgradeBundle;
    private final AtomicBoolean failed = new AtomicBoolean();

    public StandardFlowEvolutionContext(final FlowContext preUpgradeActiveFlowContext, final Bundle postUpgradeBundle) {
        this.preUpgradeActiveFlowContext = preUpgradeActiveFlowContext;
        this.postUpgradeBundle = postUpgradeBundle;
    }

    void fail() {
        failed.set(true);
    }

    boolean isFailed() {
        return failed.get();
    }

    @Override
    public FlowContext getPreUpgradeActiveFlowContext() {
        ensureOpen();
        return preUpgradeActiveFlowContext;
    }

    @Override
    public Bundle getPostUpgradeBundle() {
        ensureOpen();
        return postUpgradeBundle;
    }

    private void ensureOpen() {
        if (failed.get()) {
            throw new IllegalStateException(CLOSED_MESSAGE);
        }
    }
}
