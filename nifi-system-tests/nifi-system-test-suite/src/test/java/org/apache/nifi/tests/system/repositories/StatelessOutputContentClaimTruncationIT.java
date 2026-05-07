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
package org.apache.nifi.tests.system.repositories;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * System test that exercises the interaction between Content Claim Truncation and the Stateless
 * to framework output bridge.
 *
 * <p>When a Stateless Process Group hands its output FlowFiles back to the framework,
 * {@code StatelessFlowTask.createOutputRecords} delegates to
 * {@code ConnectionUtils.createRepositoryRecord}, which constructs each output
 * {@code StandardRepositoryRecord} as an {@code UPDATE} record without marking the record as
 * content-modified. As a result, when {@code WriteAheadFlowFileRepository.updateRepository}
 * processes the batch, the truncation reference count for the output FlowFiles' Content Claim is
 * never incremented. The framework's view of that Content Claim therefore retains only the count
 * that was established for the input FlowFile when it was originally created upstream of the
 * Stateless group.</p>
 *
 * <p>If the input FlowFile inside the Stateless group is split via {@code session.clone(offset, size)}
 * into many children that all share the same Content Claim, and then the input FlowFile is removed
 * inside the Stateless group, the framework records the input FlowFile as a {@code DELETE} alongside
 * the children's {@code UPDATE}-without-content-modified records. The {@code DELETE} decrements the
 * truncation reference count for the shared Content Claim to zero while the children still reference
 * offsets inside it. The Content Claim is queued for truncation and its on-disk Resource Claim file
 * is shrunk back to {@code claim.getOffset()} by the periodic truncate task, destroying every byte
 * the surviving children depend on.</p>
 *
 * <p>This test reproduces that scenario end-to-end. A pre-warm phase ensures the Resource Claim
 * reused for the input FlowFile begins at {@code offset > 0}. A multi-line input FlowFile larger than
 * the truncation threshold flows into a Stateless group that splits it via {@code session.clone}. The
 * children land in a downstream queue and the test then asserts that every child still returns its
 * original line bytes after the truncate task has had time to run.</p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class StatelessOutputContentClaimTruncationIT extends NiFiSystemIT {

    private static final int LINE_BYTES = 2 * 1024;
    private static final int LINE_COUNT = 30;
    private static final String LINE_TEXT = "x".repeat(LINE_BYTES);

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(
                "nifi.flowfile.repository.checkpoint.interval", "1 sec",
                "nifi.content.repository.archive.cleanup.frequency", "1 sec",
                "nifi.content.claim.max.appendable.size", "50 KB",
                "nifi.content.claim.truncation.enabled", "true",
                "nifi.content.repository.archive.max.usage.percentage", "1%");
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testStatelessOutputContentNotPrematurelyTruncated() throws Exception {
        // Pre-warm the writableClaimQueue with a partially-filled Resource Claim. The pre-warm
        // FlowFile is queued upstream of an unstarted TerminateFlowFile so the Resource Claim file
        // remains writable and only a few bytes long. The next ContentRepository.create() call will
        // reuse this Resource Claim and the resulting ContentClaim will have offset > 0.
        final ProcessorEntity prewarmGenerator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(prewarmGenerator, Map.of(
                "Text", "warmup",
                "Batch Size", "1",
                "Max FlowFiles", "1"));
        getClientUtil().updateProcessorSchedulingPeriod(prewarmGenerator, "0 sec");

        final ProcessorEntity prewarmTerminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity prewarmConnection = getClientUtil().createConnection(prewarmGenerator, prewarmTerminate, "success");

        getClientUtil().startProcessor(prewarmGenerator);
        waitForQueueCount(prewarmConnection.getId(), 1);
        getClientUtil().stopProcessor(prewarmGenerator);

        // Build the multi-line content. Each line is LINE_BYTES of 'x' followed by a unique numeric
        // suffix and a newline. The unique suffix lets the assertion identify which line each child
        // FlowFile carries. The total content size of LINE_COUNT * (LINE_BYTES + suffix + '\n') is
        // well above the 50 KB truncation threshold, so the input FlowFile's ContentClaim becomes a
        // truncation candidate once the writableClaimQueue is pre-warmed.
        final StringBuilder contentBuilder = new StringBuilder();
        final String[] expectedLines = new String[LINE_COUNT];
        for (int lineIndex = 0; lineIndex < LINE_COUNT; lineIndex++) {
            final String line = LINE_TEXT + "-" + lineIndex;
            expectedLines[lineIndex] = line;
            contentBuilder.append(line).append('\n');
        }
        final String inputContent = contentBuilder.toString();

        // Source generator produces a single multi-line FlowFile. Connecting it to the Stateless
        // group's input port enrolls the FlowFile in the framework's FlowFile repository with a
        // CREATE record that increments the truncation reference count for the new ContentClaim.
        final ProcessorEntity sourceGenerator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(sourceGenerator, Map.of(
                "Text", inputContent,
                "Batch Size", "1",
                "Max FlowFiles", "1"));
        getClientUtil().updateProcessorSchedulingPeriod(sourceGenerator, "0 sec");

        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min");

        final PortEntity inputPort = getClientUtil().createInputPort("In", statelessGroup.getId());
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", statelessGroup.getId());

        // Inside the Stateless group, SplitByLine with Use Clone = true uses session.clone(offset,
        // size) so every child FlowFile shares the input FlowFile's ContentClaim, with each child's
        // FlowFile-level offset pointing into a different region of that shared claim.
        final ProcessorEntity splitByLine = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(splitByLine, Map.of("Use Clone", "true"));

        final ConnectionEntity sourceToInput = getClientUtil().createConnection(sourceGenerator, inputPort, "success");
        getClientUtil().createConnection(inputPort, splitByLine, statelessGroup.getId());
        getClientUtil().createConnection(splitByLine, outputPort, "success", statelessGroup.getId());

        // The output queue is connected to an unstarted TerminateFlowFile so children stay queued
        // and remain readable for the assertion phase. The act of bridging the children back to the
        // framework from the Stateless group is itself sufficient to expose the bug; no manual
        // delete on the output side is required.
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().waitForValidProcessor(sourceGenerator.getId());
        getClientUtil().waitForValidProcessor(splitByLine.getId());

        getClientUtil().startProcessor(sourceGenerator);
        waitForQueueCount(sourceToInput.getId(), 1);
        getClientUtil().stopProcessor(sourceGenerator);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        // Wait for the Stateless invocation to land all of its children in the framework queue.
        // Internally this is the moment StatelessFlowTask calls flowFileRepository.updateRepository
        // with one DELETE record for the input FlowFile and LINE_COUNT UPDATE-without-modified
        // records for the children. With the bug present, the shared ContentClaim's truncation
        // reference count drops to zero on this single repository call and the claim is enqueued
        // for truncation by the periodic TruncateClaims task.
        waitForQueueCount(outputToTerminate.getId(), LINE_COUNT);
        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());

        // Allow the periodic TruncateClaims task to run. With the bug present, this is when the
        // shared Resource Claim file is truncated back to the input FlowFile's ContentClaim offset
        // and the children's bytes are destroyed.
        Thread.sleep(5_000L);

        // Read every child and verify its content matches the corresponding line. With the bug
        // present, the underlying Resource Claim file has been shrunk to ContentClaim.getOffset()
        // and these reads return zero bytes (or fewer than expected) for every child FlowFile that
        // lived inside the truncated region of the file.
        for (int childIndex = 0; childIndex < LINE_COUNT; childIndex++) {
            final byte[] expectedContent = expectedLines[childIndex].getBytes(StandardCharsets.UTF_8);
            final byte[] actualContent = getClientUtil().getFlowFileContentAsByteArray(outputToTerminate.getId(), childIndex);
            assertArrayEquals(expectedContent, actualContent,
                    "Child FlowFile at queue index " + childIndex + " has unexpected content; expected "
                            + expectedContent.length + " bytes, got " + actualContent.length);
        }
    }
}
