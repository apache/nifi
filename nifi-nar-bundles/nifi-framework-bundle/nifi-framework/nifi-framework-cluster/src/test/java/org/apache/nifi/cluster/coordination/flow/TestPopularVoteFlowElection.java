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

package org.apache.nifi.cluster.coordination.flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestPopularVoteFlowElection {

    @Test
    public void testOnlyEmptyFlows() throws IOException {
        final FingerprintFactory fingerprintFactory = Mockito.mock(FingerprintFactory.class);
        Mockito.when(fingerprintFactory.createFingerprint(Mockito.any(byte[].class))).thenReturn("fingerprint");

        final PopularVoteFlowElection election = new PopularVoteFlowElection(1, TimeUnit.MINUTES, 3, fingerprintFactory);
        final byte[] flow = Files.readAllBytes(Paths.get("src/test/resources/conf/empty-flow.xml"));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());
        assertNull(election.castVote(createDataFlow(flow), createNodeId(1)));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());
        assertNull(election.castVote(createDataFlow(flow), createNodeId(2)));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());

        final DataFlow electedDataFlow = election.castVote(createDataFlow(flow), createNodeId(3));
        assertNotNull(electedDataFlow);

        assertEquals(new String(flow), new String(electedDataFlow.getFlow()));
    }

    @Test
    public void testDifferentEmptyFlows() throws IOException {
        final FingerprintFactory fingerprintFactory = Mockito.mock(FingerprintFactory.class);
        Mockito.when(fingerprintFactory.createFingerprint(Mockito.any(byte[].class))).thenAnswer(new Answer<String>() {
            @Override
            public String answer(final InvocationOnMock invocation) throws Throwable {
                final byte[] flow = invocation.getArgumentAt(0, byte[].class);
                final String xml = new String(flow);

                // Return the ID of the root group as the fingerprint.
                final String fingerprint = xml.replaceAll("(?s:(.*<id>)(.*?)(</id>.*))", "$2");
                return fingerprint;
            }
        });

        final PopularVoteFlowElection election = new PopularVoteFlowElection(1, TimeUnit.MINUTES, 3, fingerprintFactory);
        final byte[] flow1 = Files.readAllBytes(Paths.get("src/test/resources/conf/empty-flow.xml"));
        final byte[] flow2 = Files.readAllBytes(Paths.get("src/test/resources/conf/different-empty-flow.xml"));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());
        assertNull(election.castVote(createDataFlow(flow1), createNodeId(1)));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());
        assertNull(election.castVote(createDataFlow(flow1), createNodeId(2)));

        assertFalse(election.isElectionComplete());
        assertNull(election.getElectedDataFlow());

        final DataFlow electedDataFlow = election.castVote(createDataFlow(flow2), createNodeId(3));
        assertNotNull(electedDataFlow);

        final String electedFlowXml = new String(electedDataFlow.getFlow());
        assertTrue(new String(flow1).equals(electedFlowXml) || new String(flow2).equals(electedFlowXml));
    }


    @Test
    public void testEmptyFlowIgnoredIfNonEmptyFlowExists() throws IOException {
        final FingerprintFactory fingerprintFactory = Mockito.mock(FingerprintFactory.class);
        Mockito.when(fingerprintFactory.createFingerprint(Mockito.any(byte[].class))).thenReturn("fingerprint");

        final PopularVoteFlowElection election = new PopularVoteFlowElection(1, TimeUnit.MINUTES, 8, fingerprintFactory);
        final byte[] emptyFlow = Files.readAllBytes(Paths.get("src/test/resources/conf/empty-flow.xml"));
        final byte[] nonEmptyFlow = Files.readAllBytes(Paths.get("src/test/resources/conf/non-empty-flow.xml"));

        for (int i = 0; i < 8; i++) {
            assertFalse(election.isElectionComplete());
            assertNull(election.getElectedDataFlow());

            final DataFlow dataFlow;
            if (i % 4 == 0) {
                dataFlow = createDataFlow(nonEmptyFlow);
            } else {
                dataFlow = createDataFlow(emptyFlow);
            }

            final DataFlow electedDataFlow = election.castVote(dataFlow, createNodeId(i));
            if (i == 7) {
                assertNotNull(electedDataFlow);
                assertEquals(new String(nonEmptyFlow), new String(electedDataFlow.getFlow()));
            } else {
                assertNull(electedDataFlow);
            }
        }
    }


    private NodeIdentifier createNodeId(final int index) {
        return new NodeIdentifier(UUID.randomUUID().toString(), "localhost", 9000 + index, "localhost", 9000 + index, "localhost", 9000 + index, 9000 + index, true);
    }

    private DataFlow createDataFlow(final byte[] flow) {
        return new StandardDataFlow(flow, new byte[0], new byte[0]);
    }
}
