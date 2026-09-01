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

package org.apache.nifi.tests.system.stateless;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StatelessInMemoryContentIT extends NiFiSystemIT {

    private static final String HELLO_WORLD = "Hello World";
    private static final String EXCLAMATIONS = "!!!";

    // Result of reversing "Hello World" to "dlroW olleH", appending "!!!", then reversing "dlroW olleH!!!" back.
    private static final String TRANSFORMED = EXCLAMATIONS + HELLO_WORLD;

    private static final String INHERITED = "INHERITED";
    private static final String CONTENT_REPOSITORY = "CONTENT_REPOSITORY";
    private static final String IN_MEMORY = "IN_MEMORY";

    @Test
    public void testContentProcessedInMemoryWithoutWritingToDisk() throws NiFiClientException, IOException, InterruptedException {
        // A Process Group that buffers FlowFile content in memory must be disconnected from all other components, so the entire flow is self-contained.
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min", IN_MEMORY);
        final String groupId = statelessGroup.getId();

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, groupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", HELLO_WORLD));

        // Modify the content several times so it is written to and read back from the in-memory Content Repository repeatedly.
        final ProcessorEntity reverseFirst = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);
        final ProcessorEntity append = getClientUtil().createProcessor("UpdateContent", groupId);
        getClientUtil().updateProcessorProperties(append, Map.of("Content", EXCLAMATIONS, "Update Strategy", "Append"));
        final ProcessorEntity reverseSecond = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);

        // VerifyContents routes to a "matched" relationship when the content equals the expected value, otherwise to "unmatched".
        final ProcessorEntity verify = getClientUtil().createProcessor("VerifyContents", groupId);
        getClientUtil().updateProcessorProperties(verify, Map.of("matched", TRANSFORMED));
        final ProcessorEntity matchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);
        final ProcessorEntity unmatchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);

        getClientUtil().createConnection(generate, reverseFirst, SUCCESS, groupId);
        getClientUtil().createConnection(reverseFirst, append, SUCCESS, groupId);
        getClientUtil().createConnection(append, reverseSecond, SUCCESS, groupId);
        getClientUtil().createConnection(reverseSecond, verify, SUCCESS, groupId);
        getClientUtil().createConnection(verify, matchedTerminate, "matched", groupId);
        getClientUtil().createConnection(verify, unmatchedTerminate, "unmatched", groupId);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(reverseFirst.getId());
        getClientUtil().waitForValidProcessor(append.getId());
        getClientUtil().waitForValidProcessor(reverseSecond.getId());
        getClientUtil().waitForValidProcessor(verify.getId());

        getClientUtil().startProcessGroupComponents(groupId);

        // The content flows through several in-memory read/write cycles and must arrive at the "matched" relationship with the expected value.
        waitFor(() -> getProcessorFlowFilesIn(matchedTerminate.getId()) >= 1);
        getClientUtil().stopProcessGroupComponents(groupId);

        // No FlowFile should have reached the "unmatched" relationship, proving the content was correct after the in-memory modifications.
        assertEquals(0, getProcessorFlowFilesIn(unmatchedTerminate.getId()));

        // Because the group buffers content in memory, nothing should have been written to the on-disk Content Repository.
        final File contentRepository = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        assertEquals(0L, contentBytesWrittenToDisk(contentRepository));
    }

    @Test
    public void testInMemoryNotAllowedWhenGroupHasConnections() throws NiFiClientException, IOException {
        // A group with an incoming connection cannot buffer content in memory.
        final ProcessGroupEntity incomingGroup = getClientUtil().createProcessGroup("IncomingConnected", "root");
        final PortEntity inputPort = getClientUtil().createInputPort("In", incomingGroup.getId());
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        getClientUtil().createConnection(generate, inputPort, SUCCESS);
        assertThrows(NiFiClientException.class, () -> getClientUtil().markStateless(incomingGroup, "1 min", IN_MEMORY));

        // A group with an outgoing connection cannot buffer content in memory.
        final ProcessGroupEntity outgoingGroup = getClientUtil().createProcessGroup("OutgoingConnected", "root");
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", outgoingGroup.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        getClientUtil().createConnection(outputPort, terminate);
        assertThrows(NiFiClientException.class, () -> getClientUtil().markStateless(outgoingGroup, "1 min", IN_MEMORY));
    }

    @Test
    public void testCannotConnectToInMemoryGroup() throws NiFiClientException, IOException {
        // A group with ports but no connections may buffer content in memory.
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        final PortEntity inputPort = getClientUtil().createInputPort("In", statelessGroup.getId());
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", statelessGroup.getId());
        getClientUtil().markStateless(statelessGroup, "1 min", IN_MEMORY);

        // Once configured to buffer content in memory, connecting a component into or out of the group is not allowed.
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        assertThrows(NiFiClientException.class, () -> getClientUtil().createConnection(generate, inputPort, SUCCESS));

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        assertThrows(NiFiClientException.class, () -> getClientUtil().createConnection(outputPort, terminate));
    }

    @Test
    public void testCannotConfigureChildDifferentlyFromStatelessParent() throws NiFiClientException, IOException {
        final ProcessGroupEntity parent = getClientUtil().createProcessGroup("Parent", "root");
        getClientUtil().markStateless(parent, "1 min", CONTENT_REPOSITORY);

        final ProcessGroupEntity child = getClientUtil().createProcessGroup("Child", parent.getId());

        // The child resolves to the parent's Stateless Execution Engine, so it cannot buffer content in memory while the parent uses the Content Repository.
        assertThrows(NiFiClientException.class, () -> getClientUtil().setStatelessContentStorageLocation(child, IN_MEMORY));
    }

    @Test
    public void testCannotMoveInMemoryGroupIntoContentRepositoryParent() throws NiFiClientException, IOException {
        final ProcessGroupEntity inMemoryGroup = getClientUtil().createProcessGroup("InMemoryGroup", "root");
        getClientUtil().markStateless(inMemoryGroup, "1 min", IN_MEMORY);

        final ProcessGroupEntity contentRepoParent = getClientUtil().createProcessGroup("ContentRepositoryParent", "root");
        getClientUtil().markStateless(contentRepoParent, "1 min", CONTENT_REPOSITORY);

        final ProcessGroupEntity toMove = getNifiClient().getProcessGroupClient().getProcessGroup(inMemoryGroup.getId());
        assertThrows(NiFiClientException.class, () -> getClientUtil().moveProcessGroup(toMove, contentRepoParent.getId()));

        // Once the group inherits its content storage, the move is allowed and it takes on the parent's Content Repository setting.
        getClientUtil().setStatelessContentStorageLocation(inMemoryGroup, INHERITED);
        final ProcessGroupEntity inheritedGroup = getNifiClient().getProcessGroupClient().getProcessGroup(inMemoryGroup.getId());
        getClientUtil().moveProcessGroup(inheritedGroup, contentRepoParent.getId());

        final ProcessGroupEntity moved = getNifiClient().getProcessGroupClient().getProcessGroup(inMemoryGroup.getId());
        assertEquals(contentRepoParent.getId(), moved.getComponent().getParentGroupId());
    }

    @Test
    public void testCannotChangeContentStorageWhileRunning() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min", IN_MEMORY);
        final String groupId = statelessGroup.getId();

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, groupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", HELLO_WORLD));
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);
        getClientUtil().createConnection(generate, terminate, SUCCESS, groupId);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessGroupComponents(groupId);

        waitFor(() -> getProcessorFlowFilesIn(terminate.getId()) >= 1);

        // The Content Repository is chosen when the Stateless flow starts, so the location cannot be changed while the group is running.
        assertThrows(NiFiClientException.class, () -> getClientUtil().setStatelessContentStorageLocation(statelessGroup, CONTENT_REPOSITORY));

        getClientUtil().stopProcessGroupComponents(groupId);

        final ProcessGroupEntity stoppedGroup = getClientUtil().setStatelessContentStorageLocation(statelessGroup, CONTENT_REPOSITORY);
        assertEquals(CONTENT_REPOSITORY, stoppedGroup.getComponent().getStatelessContentStorageLocation());
    }

    private int getProcessorFlowFilesIn(final String processorId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessorClient().getProcessor(processorId).getStatus().getAggregateSnapshot().getFlowFilesIn();
    }

    private long contentBytesWrittenToDisk(final File contentRepository) throws IOException {
        if (!contentRepository.exists()) {
            return 0L;
        }

        try (final Stream<Path> paths = Files.walk(contentRepository.toPath())) {
            return paths.filter(Files::isRegularFile)
                .mapToLong(path -> path.toFile().length())
                .sum();
        }
    }
}
