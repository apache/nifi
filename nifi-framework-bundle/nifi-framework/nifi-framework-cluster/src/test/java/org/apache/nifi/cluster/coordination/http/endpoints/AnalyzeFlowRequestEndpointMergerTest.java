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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.web.api.dto.AnalyzeFlowRequestDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

public class AnalyzeFlowRequestEndpointMergerTest {

    private AnalyzeFlowRequestEndpointMerger testSubject;

    @BeforeEach
    public void setUp() throws Exception {
        testSubject = new AnalyzeFlowRequestEndpointMerger();
    }

    @Test
    public void testAllRequestsWaiting() throws Exception {
        // GIVEN
        final AnalyzeFlowRequestDTO clientDto = createRequest(false, null, "WAITING");

        final Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap = new HashMap<>();
        dtoMap.put(mock(NodeIdentifier.class), createRequest(false, null, "WAITING"));
        dtoMap.put(mock(NodeIdentifier.class), createRequest(false, null, "WAITING"));

        final AnalyzeFlowRequestDTO expected = createRequest(false, null, "WAITING");

        // WHEN
        testSubject.mergeResponses(clientDto, dtoMap, null, null);

        // THEN
        this.assertEquals(expected, clientDto);
    }

    @Test
    public void testClientRequestWaitingOthersComplete() throws Exception {
        // GIVEN
        final AnalyzeFlowRequestDTO clientDto = createRequest(false, null, "WAITING");

        final Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap = new HashMap<>();
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, null, "COMPLETE"));
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, null, "COMPLETE"));

        final AnalyzeFlowRequestDTO expected = createRequest(false, null, "WAITING");

        // WHEN
        testSubject.mergeResponses(clientDto, dtoMap, null, null);

        // THEN
        this.assertEquals(expected, clientDto);
    }

    @Test
    public void testOneNonClientRequestCompleteOthersWaiting() throws Exception {
        // GIVEN
        final AnalyzeFlowRequestDTO clientDto = createRequest(false, null, "WAITING");

        final Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap = new HashMap<>();
        dtoMap.put(mock(NodeIdentifier.class), createRequest(false, null, "WAITING"));
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, null, "COMPLETE"));

        final AnalyzeFlowRequestDTO expected = createRequest(false, null, "WAITING");

        // WHEN
        testSubject.mergeResponses(clientDto, dtoMap, null, null);

        // THEN
        this.assertEquals(expected, clientDto);
    }

    @Test
    public void testAllRequestsComplete() throws Exception {
        // GIVEN
        final AnalyzeFlowRequestDTO clientDto = createRequest(true, null, "COMPLETE");

        final Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap = new HashMap<>();
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, null, "COMPLETE"));
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, null, "COMPLETE"));

        final AnalyzeFlowRequestDTO expected = createRequest(true, null, "COMPLETE");

        // WHEN
        testSubject.mergeResponses(clientDto, dtoMap, null, null);

        // THEN
        this.assertEquals(expected, clientDto);
    }

    @Test
    public void testMergeFailures() throws Exception {
        // GIVEN
        final AnalyzeFlowRequestDTO clientDto = createRequest(true, "failure1", "FAILURE");

        final Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap = new HashMap<>();
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, "failure2", "FAILURE"));
        dtoMap.put(mock(NodeIdentifier.class), createRequest(true, "failure3", "FAILURE"));

        final Set<String> expectedFailures = new HashSet<>(Arrays.asList("failure1", "failure2", "failure3"));

        // WHEN
        testSubject.mergeResponses(clientDto, dtoMap, null, null);

        // THEN
        final Set<String> actualFailures = new HashSet<>(Arrays.asList(clientDto.getFailureReason().split("\n")));

        Assertions.assertEquals(expectedFailures, actualFailures);
    }

    private AnalyzeFlowRequestDTO createRequest(
        final Boolean complete,
        final String failureReason,
        final String state
    ) {
        final AnalyzeFlowRequestDTO request = new AnalyzeFlowRequestDTO();

        request.setComplete(complete);
        request.setFailureReason(failureReason);
        request.setState(state);

        return request;
    }

    private void assertEquals(final AnalyzeFlowRequestDTO expected, final AnalyzeFlowRequestDTO clientDto) {
        final List<Function<AnalyzeFlowRequestDTO, Object>> equalityCheckers = Arrays.asList(
            AnalyzeFlowRequestDTO::isComplete,
            AnalyzeFlowRequestDTO::getFailureReason,
            AnalyzeFlowRequestDTO::getState
        );

        Assertions.assertEquals(
            new EqualsWrapper<>(expected, equalityCheckers),
            new EqualsWrapper<>(clientDto, equalityCheckers)
        );
    }
}
