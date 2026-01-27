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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.StandardConnectorInitializationContext;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

public class MockConnectorInitializationContext extends StandardConnectorInitializationContext {

    private final MockExtensionMapper mockExtensionMapper;

    protected MockConnectorInitializationContext(final Builder builder) {
        super(builder);
        this.mockExtensionMapper = builder.mockExtensionMapper;
    }

    @Override
    public void updateFlow(final FlowContext flowContext, final VersionedExternalFlow versionedExternalFlow,
                           final BundleCompatibility bundleCompatability) throws FlowUpdateException {
        replaceMocks(versionedExternalFlow.getFlowContents());
        super.updateFlow(flowContext, versionedExternalFlow, bundleCompatability);
    }

    private void replaceMocks(final VersionedProcessGroup group) {
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                mockExtensionMapper.mapProcessor(processor);
            }
        }

        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                replaceMocks(childGroup);
            }
        }
    }


    public static class Builder extends StandardConnectorInitializationContext.Builder {
        private final MockExtensionMapper mockExtensionMapper;

        public Builder(final MockExtensionMapper mockExtensionMapper) {
            this.mockExtensionMapper = mockExtensionMapper;
        }

        @Override
        public MockConnectorInitializationContext build() {
            return new MockConnectorInitializationContext(this);
        }
    }
}
