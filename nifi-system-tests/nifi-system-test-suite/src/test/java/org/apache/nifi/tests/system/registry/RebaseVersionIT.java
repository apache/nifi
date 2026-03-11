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

package org.apache.nifi.tests.system.registry;

import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.RebaseChangeDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RebaseAnalysisEntity;
import org.apache.nifi.web.api.entity.RebaseRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RebaseVersionIT extends NiFiSystemIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";

    @Test
    public void testCleanRebaseWithPositionAndPropertyChanges() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "RebaseFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("Batch Size", "5"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "99 B"));

        final RebaseAnalysisEntity analysis = util.getRebaseAnalysis(originalGroup.getId(), "2");
        assertTrue(analysis.getRebaseAllowed(), "Expected rebase to be allowed but it was not. Failure: " + analysis.getFailureReason()
                + ". Local changes: " + describeLocalChanges(analysis));

        util.rebaseFlowVersion(originalGroup.getId(), "2");

        final VersionControlInformationDTO updatedVci = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", updatedVci.getVersion());

        final ProcessorEntity rebasedProcessor = findSingleProcessor(originalGroup.getId());
        final Map<String, String> properties = rebasedProcessor.getComponent().getConfig().getProperties();
        assertEquals("99 B", properties.get("File Size"));
        assertEquals("5", properties.get("Batch Size"));
    }

    @Test
    public void testRejectedRebaseDueToConflictingProperty() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "ConflictFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("File Size", "20 B"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "50 B"));

        final RebaseAnalysisEntity analysis = util.getRebaseAnalysis(originalGroup.getId(), "2");
        assertFalse(analysis.getRebaseAllowed());

        final boolean hasConflicting = analysis.getLocalChanges().stream()
                .map(RebaseChangeDTO::getClassification)
                .anyMatch("CONFLICTING"::equals);
        assertTrue(hasConflicting);
    }

    @Test
    public void testRejectedRebaseDueToUnsupportedChangeType() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "UnsupportedFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("File Size", "20 B"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.createProcessor("TerminateFlowFile", originalGroup.getId());

        final RebaseAnalysisEntity analysis = util.getRebaseAnalysis(originalGroup.getId(), "2");
        assertFalse(analysis.getRebaseAllowed());

        final boolean hasUnsupported = analysis.getLocalChanges().stream()
                .map(RebaseChangeDTO::getClassification)
                .anyMatch("UNSUPPORTED"::equals);
        assertTrue(hasUnsupported);
    }

    @Test
    public void testRebaseFollowedByCommit() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "CommitAfterRebase");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("Batch Size", "5"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "99 B"));

        util.rebaseFlowVersion(originalGroup.getId(), "2");

        final VersionControlInformationDTO afterRebase = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", afterRebase.getVersion());

        util.saveFlowVersion(originalGroup, clientEntity, getVersionControlInformation(originalGroup.getId()));

        final VersionControlInformationDTO committedVci = getVersionControlInfo(originalGroup.getId());
        assertEquals("3", committedVci.getVersion());
    }

    @Test
    public void testRebaseFollowedByRevert() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "RevertAfterRebase");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("Batch Size", "5"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "99 B"));

        util.rebaseFlowVersion(originalGroup.getId(), "2");

        final VersionControlInformationDTO afterRebase = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", afterRebase.getVersion());

        util.revertChanges(originalGroup);

        final VersionControlInformationDTO revertedVci = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", revertedVci.getVersion());
    }

    @Test
    public void testMultiVersionJump() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "MultiVersionFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");

        final ProcessorEntity v2Processor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(v2Processor, Map.of("File Size", "20 B"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        final ProcessorEntity v3Processor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(v3Processor, Map.of("Batch Size", "3"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        final ProcessorEntity v4Processor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(v4Processor, Map.of("Max FlowFiles", "10"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("Text", "hello"));

        final RebaseAnalysisEntity analysis = util.getRebaseAnalysis(originalGroup.getId(), "4");
        assertTrue(analysis.getRebaseAllowed(), "Expected rebase to be allowed but it was not. Failure: " + analysis.getFailureReason()
                + ". Local changes: " + describeLocalChanges(analysis));

        util.rebaseFlowVersion(originalGroup.getId(), "4");

        final VersionControlInformationDTO updatedVci = getVersionControlInfo(originalGroup.getId());
        assertEquals("4", updatedVci.getVersion());

        final ProcessorEntity rebasedProcessor = findSingleProcessor(originalGroup.getId());
        final Map<String, String> properties = rebasedProcessor.getComponent().getConfig().getProperties();
        assertEquals("hello", properties.get("Text"));
        assertEquals("20 B", properties.get("File Size"));
        assertEquals("3", properties.get("Batch Size"));
        assertEquals("10", properties.get("Max FlowFiles"));
    }

    @Test
    public void testStaleAnalysisFingerprintRejection() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "FingerprintFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("Batch Size", "5"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "99 B"));

        final RebaseAnalysisEntity initialAnalysis = util.getRebaseAnalysis(originalGroup.getId(), "2");
        final String staleFingerprint = initialAnalysis.getAnalysisFingerprint();

        util.updateProcessorProperties(generate, Map.of("Max FlowFiles", "50"));

        boolean rebaseWithStaleFailed = false;
        try {
            executeRebaseWithFingerprint(originalGroup, "2", staleFingerprint);
        } catch (final Exception e) {
            rebaseWithStaleFailed = true;
        }
        assertTrue(rebaseWithStaleFailed);

        util.rebaseFlowVersion(originalGroup.getId(), "2");

        final VersionControlInformationDTO updatedVci = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", updatedVci.getVersion());
    }

    @Test
    public void testNestedVersionedPGWithLocalModificationsBlocksRebase() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity parentGroup = util.createProcessGroup("Parent", "root");
        util.createProcessor("GenerateFlowFile", parentGroup.getId());

        final ProcessGroupEntity childGroup = util.createProcessGroup("Child", parentGroup.getId());
        final ProcessorEntity childProcessor = util.createProcessor("TerminateFlowFile", childGroup.getId());
        util.startVersionControl(childGroup, clientEntity, TEST_FLOWS_BUCKET, "ChildFlow");

        final VersionControlInformationEntity parentVci = util.startVersionControl(parentGroup, clientEntity, TEST_FLOWS_BUCKET, "ParentFlow");
        final String parentFlowId = parentVci.getVersionControlInformation().getFlowId();

        util.updateProcessorProperties(childProcessor, Map.of("Response Status", "200"));

        final ProcessGroupEntity secondParent = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, parentFlowId, "1");
        final ProcessorEntity secondParentProcessor = findProcessorByType(secondParent.getId(), "GenerateFlowFile");
        util.updateProcessorProperties(secondParentProcessor, Map.of("File Size", "20 B"));
        util.saveFlowVersion(secondParent, clientEntity, getVersionControlInformation(secondParent.getId()));

        final RebaseAnalysisEntity analysis = util.getRebaseAnalysis(parentGroup.getId(), "2");
        assertFalse(analysis.getRebaseAllowed(), "Rebase should be blocked due to descendant modifications");
        assertNotNull(analysis.getFailureReason());
    }

    @Test
    public void testRebaseThenRebaseAgain() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity originalGroup = util.createProcessGroup("Original", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", originalGroup.getId());

        final VersionControlInformationEntity vci = util.startVersionControl(originalGroup, clientEntity, TEST_FLOWS_BUCKET, "DoubleRebaseFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity secondGroup = util.importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity secondGroupProcessor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(secondGroupProcessor, Map.of("Batch Size", "5"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.updateProcessorProperties(generate, Map.of("File Size", "99 B"));
        util.rebaseFlowVersion(originalGroup.getId(), "2");

        final VersionControlInformationDTO afterFirstRebase = getVersionControlInfo(originalGroup.getId());
        assertEquals("2", afterFirstRebase.getVersion());

        final ProcessorEntity v3Processor = findSingleProcessor(secondGroup.getId());
        util.updateProcessorProperties(v3Processor, Map.of("Max FlowFiles", "30"));
        util.saveFlowVersion(secondGroup, clientEntity, getVersionControlInformation(secondGroup.getId()));

        util.rebaseFlowVersion(originalGroup.getId(), "3");

        final VersionControlInformationDTO afterSecondRebase = getVersionControlInfo(originalGroup.getId());
        assertEquals("3", afterSecondRebase.getVersion());

        final ProcessorEntity rebasedProcessor = findSingleProcessor(originalGroup.getId());
        final Map<String, String> properties = rebasedProcessor.getComponent().getConfig().getProperties();
        assertEquals("99 B", properties.get("File Size"));
        assertEquals("5", properties.get("Batch Size"));
        assertEquals("30", properties.get("Max FlowFiles"));
    }

    private String describeLocalChanges(final RebaseAnalysisEntity analysis) {
        if (analysis.getLocalChanges() == null || analysis.getLocalChanges().isEmpty()) {
            return "none";
        }
        final StringBuilder sb = new StringBuilder();
        for (final RebaseChangeDTO change : analysis.getLocalChanges()) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append("[%s %s on %s = %s]".formatted(change.getClassification(), change.getDifferenceType(), change.getComponentName(), change.getConflictDetail()));
        }
        return sb.toString();
    }

    private VersionControlInformationDTO getVersionControlInfo(final String processGroupId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessGroupClient().getProcessGroup(processGroupId)
                .getComponent().getVersionControlInformation();
    }

    private VersionControlInformationEntity getVersionControlInformation(final String processGroupId) throws NiFiClientException, IOException {
        return getNifiClient().getVersionsClient().getVersionControlInfo(processGroupId);
    }

    private ProcessorEntity findSingleProcessor(final String processGroupId) throws NiFiClientException, IOException {
        final FlowDTO flow = getNifiClient().getFlowClient().getProcessGroup(processGroupId).getProcessGroupFlow().getFlow();
        final Set<ProcessorEntity> processors = flow.getProcessors();
        assertEquals(1, processors.size());
        return processors.iterator().next();
    }

    private ProcessorEntity findProcessorByType(final String processGroupId, final String simpleTypeName) throws NiFiClientException, IOException {
        final FlowDTO flow = getNifiClient().getFlowClient().getProcessGroup(processGroupId).getProcessGroupFlow().getFlow();
        return flow.getProcessors().stream()
                .filter(proc -> proc.getComponent().getType().endsWith("." + simpleTypeName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No processor of type " + simpleTypeName + " found in group " + processGroupId));
    }

    private void executeRebaseWithFingerprint(final ProcessGroupEntity group, final String targetVersion, final String fingerprint)
            throws NiFiClientException, IOException, InterruptedException {

        final ProcessGroupEntity groupEntity = getNifiClient().getProcessGroupClient().getProcessGroup(group.getId());
        final VersionControlInformationDTO vciDto = groupEntity.getComponent().getVersionControlInformation();
        vciDto.setVersion(targetVersion);

        final VersionControlInformationEntity vciEntity = new VersionControlInformationEntity();
        vciEntity.setProcessGroupRevision(groupEntity.getRevision());
        vciEntity.setVersionControlInformation(vciDto);

        final RebaseRequestEntity rebaseRequest = new RebaseRequestEntity();
        rebaseRequest.setVersionControlInformationEntity(vciEntity);
        rebaseRequest.setAnalysisFingerprint(fingerprint);

        final VersionedFlowUpdateRequestEntity result = getNifiClient().getVersionsClient().initiateRebase(group.getId(), rebaseRequest);
        final String requestId = result.getRequest().getRequestId();

        while (true) {
            final VersionedFlowUpdateRequestEntity entity = getNifiClient().getVersionsClient().getRebaseRequest(requestId);
            if (entity.getRequest().isComplete()) {
                if (entity.getRequest().getFailureReason() != null) {
                    throw new RuntimeException("Rebase failed: " + entity.getRequest().getFailureReason());
                }
                return;
            }
            Thread.sleep(100L);
        }
    }
}
