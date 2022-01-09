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
package org.apache.nifi.web.controller;

import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SearchResultMatcher {
    private final Set<ComponentSearchResultDTO> outputPortResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> inputPortResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> processorResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> labelResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> remoteProcessGroupResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> funnelResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> connectionResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> processGroupResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> parameterContextResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> parameterResults = new HashSet<>();
    private final Set<ComponentSearchResultDTO> controllerServiceNodeResults = new HashSet<>();

    public SearchResultMatcher ofOutputPort(final ComponentSearchResultDTO result) {
        outputPortResults.add(result);
        return this;
    }

    public SearchResultMatcher ofInputPort(final ComponentSearchResultDTO result) {
        inputPortResults.add(result);
        return this;
    }

    public SearchResultMatcher ofProcessor(final ComponentSearchResultDTO result) {
        processorResults.add(result);
        return this;
    }

    public SearchResultMatcher ofLabel(final ComponentSearchResultDTO result) {
        labelResults.add(result);
        return this;
    }

    public SearchResultMatcher ofRemoteProcessGroup(final ComponentSearchResultDTO result) {
        remoteProcessGroupResults.add(result);
        return this;
    }

    public SearchResultMatcher ofFunnel(final ComponentSearchResultDTO result) {
        funnelResults.add(result);
        return this;
    }

    public SearchResultMatcher ofConnection(final ComponentSearchResultDTO result) {
        connectionResults.add(result);
        return this;
    }

    public SearchResultMatcher ofProcessGroup(final ComponentSearchResultDTO result) {
        processGroupResults.add(result);
        return this;
    }

    public SearchResultMatcher ofParameterContext(final ComponentSearchResultDTO result) {
        parameterContextResults.add(result);
        return this;
    }

    public SearchResultMatcher ofParameter(final ComponentSearchResultDTO result) {
        parameterResults.add(result);
        return this;
    }

    public SearchResultMatcher ofParameter(final Collection<ComponentSearchResultDTO> result) {
        parameterResults.addAll(result);
        return this;
    }

    public SearchResultMatcher ofControllerServiceNode(final ComponentSearchResultDTO result) {
        controllerServiceNodeResults.add(result);
        return this;
    }

    public void validate(final SearchResultsDTO results) {
        validate(outputPortResults, results.getOutputPortResults());
        validate(inputPortResults, results.getInputPortResults());
        validate(processorResults, results.getProcessorResults());
        validate(labelResults, results.getLabelResults());
        validate(remoteProcessGroupResults, results.getRemoteProcessGroupResults());
        validate(funnelResults, results.getFunnelResults());
        validate(connectionResults, results.getConnectionResults());
        validate(processGroupResults, results.getProcessGroupResults());
        validate(parameterContextResults, results.getParameterContextResults());
        validate(parameterResults, results.getParameterResults());
        validate(controllerServiceNodeResults, results.getControllerServiceNodeResults());
    }

    private void validate(final Collection<ComponentSearchResultDTO> expected, final Collection<ComponentSearchResultDTO> actual) {
        Set<AbstractControllerSearchIntegrationTest.ComponentSearchResultDTOWrapper> expectedConverted
                = expected.stream().map(AbstractControllerSearchIntegrationTest.ComponentSearchResultDTOWrapper::new).collect(Collectors.toSet());
        Set<AbstractControllerSearchIntegrationTest.ComponentSearchResultDTOWrapper> actualConverted
                = actual.stream().map(AbstractControllerSearchIntegrationTest.ComponentSearchResultDTOWrapper::new).collect(Collectors.toSet());

        assertEquals(expectedConverted, actualConverted);
    }
}
