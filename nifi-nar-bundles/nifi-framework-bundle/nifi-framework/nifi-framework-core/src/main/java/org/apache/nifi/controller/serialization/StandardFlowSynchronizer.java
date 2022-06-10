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

package org.apache.nifi.controller.serialization;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.XmlFlowSynchronizer;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.services.FlowService;

public class StandardFlowSynchronizer implements FlowSynchronizer {
    private final XmlFlowSynchronizer xmlFlowSynchronizer;
    private final VersionedFlowSynchronizer versionedFlowSynchronizer;

    public StandardFlowSynchronizer(final XmlFlowSynchronizer xmlFlowSynchronizer, final VersionedFlowSynchronizer versionedFlowSynchronizer) {
        this.xmlFlowSynchronizer = xmlFlowSynchronizer;
        this.versionedFlowSynchronizer = versionedFlowSynchronizer;
    }

    @Override
    public void sync(final FlowController controller, final DataFlow dataFlow, final FlowService flowService, final BundleUpdateStrategy bundleUpdateStrategy)
            throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException, MissingBundleException {

        final FlowSynchronizer synchronizer = isXml(dataFlow) ? xmlFlowSynchronizer : versionedFlowSynchronizer;
        synchronizer.sync(controller, dataFlow, flowService, bundleUpdateStrategy);
    }

    public static boolean isFlowEmpty(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        if (isXml(dataFlow)) {
            return XmlFlowSynchronizer.isFlowEmpty(dataFlow.getFlowDocument());
        } else {
            return VersionedFlowSynchronizer.isFlowEmpty(dataFlow);
        }
    }

    private static boolean isXml(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        return dataFlow.isXml();
    }
}
