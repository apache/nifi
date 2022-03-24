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
package org.apache.nifi.controller.inheritance;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.SensitiveValueEncoder;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowFingerprintCheck implements FlowInheritabilityCheck {
    private static final Logger logger = LoggerFactory.getLogger(FlowFingerprintCheck.class);

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        if (existingFlow == null) {
            return FlowInheritability.inheritable();
        }

        final byte[] existingFlowBytes = existingFlow.getFlow();
        final byte[] proposedFlowBytes = proposedFlow.getFlow();

        final PropertyEncryptor encryptor = flowController.getEncryptor();
        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final SensitiveValueEncoder sensitiveValueEncoder = flowController.getSensitiveValueEncoder();

        final FingerprintFactory fingerprintFactory = new FingerprintFactory(encryptor, extensionManager, sensitiveValueEncoder);
        final String existingFlowFingerprintBeforeHash = fingerprintFactory.createFingerprint(existingFlowBytes, flowController);
        if (existingFlowFingerprintBeforeHash.trim().isEmpty()) {
            return null;  // no existing flow, so equivalent to proposed flow
        }

        if (proposedFlow == null || proposedFlowBytes.length == 0) {
            return FlowInheritability.notInheritable("Proposed Flow was empty but Current Flow is not");  // existing flow is not empty and proposed flow is empty (we could orphan flowfiles)
        }

        final String proposedFlowFingerprintBeforeHash = fingerprintFactory.createFingerprint(proposedFlow.getFlowDocument(), flowController);
        if (proposedFlowFingerprintBeforeHash.trim().isEmpty()) {
            return FlowInheritability.notInheritable("Proposed Flow was empty but Current Flow is not");  // existing flow is not empty and proposed flow is empty (we could orphan flowfiles)
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Local Fingerprint Before Hash = {}", new Object[] {existingFlowFingerprintBeforeHash});
            logger.trace("Proposed Fingerprint Before Hash = {}", new Object[] {proposedFlowFingerprintBeforeHash});
        }

        final boolean inheritable = existingFlowFingerprintBeforeHash.equals(proposedFlowFingerprintBeforeHash);
        if (!inheritable) {
            final String discrepancy = findFirstDiscrepancy(existingFlowFingerprintBeforeHash, proposedFlowFingerprintBeforeHash, "Flows");
            return FlowInheritability.notInheritable(discrepancy);
        }

        return FlowInheritability.inheritable();
    }

    private String findFirstDiscrepancy(final String existing, final String proposed, final String comparisonDescription) {
        final int shortestFileLength = Math.min(existing.length(), proposed.length());
        for (int i = 0; i < shortestFileLength; i++) {
            if (existing.charAt(i) != proposed.charAt(i)) {
                final String formattedExistingDelta = formatFlowDiscrepancy(existing, i, 100);
                final String formattedProposedDelta = formatFlowDiscrepancy(proposed, i, 100);
                return String.format("Found difference in %s:\nLocal Fingerprint:   %s\nCluster Fingerprint: %s", comparisonDescription, formattedExistingDelta, formattedProposedDelta);
            }
        }

        // existing must startWith proposed or proposed must startWith existing
        if (existing.length() > proposed.length()) {
            final String formattedExistingDelta = existing.substring(proposed.length(), Math.min(existing.length(), proposed.length() + 200));
            return String.format("Found difference in %s:\nLocal Fingerprint contains additional configuration from Cluster Fingerprint: %s", comparisonDescription, formattedExistingDelta);
        } else if (proposed.length() > existing.length()) {
            final String formattedProposedDelta = proposed.substring(existing.length(), Math.min(proposed.length(), existing.length() + 200));
            return String.format("Found difference in %s:\nCluster Fingerprint contains additional configuration from Local Fingerprint: %s", comparisonDescription, formattedProposedDelta);
        }

        return "Unable to find any discrepancies between fingerprints. Please contact the NiFi support team";
    }

    private String formatFlowDiscrepancy(final String flowFingerprint, final int deltaIndex, final int deltaPad) {
        return flowFingerprint.substring(Math.max(0, deltaIndex - deltaPad), Math.min(flowFingerprint.length(), deltaIndex + deltaPad));
    }

}
