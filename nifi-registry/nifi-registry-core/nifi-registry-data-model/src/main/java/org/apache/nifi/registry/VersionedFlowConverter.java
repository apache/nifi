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

package org.apache.nifi.registry;

import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;

public class VersionedFlowConverter {
    public static VersionedExternalFlow createVersionedExternalFlow(final VersionedFlowSnapshot flowSnapshot) {
        final VersionedExternalFlowMetadata externalFlowMetadata = new VersionedExternalFlowMetadata();
        final VersionedFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        if (snapshotMetadata != null) {
            externalFlowMetadata.setAuthor(snapshotMetadata.getAuthor());
            externalFlowMetadata.setBucketIdentifier(snapshotMetadata.getBucketIdentifier());
            externalFlowMetadata.setComments(snapshotMetadata.getComments());
            externalFlowMetadata.setFlowIdentifier(snapshotMetadata.getFlowIdentifier());
            externalFlowMetadata.setTimestamp(snapshotMetadata.getTimestamp());
            externalFlowMetadata.setVersion(String.valueOf(snapshotMetadata.getVersion()));
        }

        final VersionedFlow versionedFlow = flowSnapshot.getFlow();
        if (versionedFlow == null) {
            externalFlowMetadata.setFlowName(flowSnapshot.getFlowContents().getName());
        } else {
            externalFlowMetadata.setFlowName(versionedFlow.getName());
        }

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(flowSnapshot.getFlowContents());
        externalFlow.setExternalControllerServices(flowSnapshot.getExternalControllerServices());
        externalFlow.setParameterContexts(flowSnapshot.getParameterContexts());
        externalFlow.setParameterProviders(flowSnapshot.getParameterProviders());
        externalFlow.setMetadata(externalFlowMetadata);

        return externalFlow;
    }
}
