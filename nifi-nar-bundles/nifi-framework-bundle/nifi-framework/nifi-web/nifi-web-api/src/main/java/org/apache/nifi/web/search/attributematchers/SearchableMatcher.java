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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.web.controller.StandardSearchContext;
import org.apache.nifi.web.search.query.SearchQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchableMatcher implements AttributeMatcher<ProcessorNode> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchableMatcher.class);

    private FlowController flowController;
    private VariableRegistry variableRegistry;

    @Override
    public void match(final ProcessorNode component, final SearchQuery query, final List<String> matches) {
        final Processor processor = component.getProcessor();

        if (processor instanceof Searchable) {
            final Searchable searchable = (Searchable) processor;
            final String searchTerm = query.getTerm();
            final SearchContext context = new StandardSearchContext(searchTerm, component, flowController.getControllerServiceProvider(), variableRegistry);

            // search the processor using the appropriate thread context classloader
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), component.getClass(), component.getIdentifier())) {
                searchable.search(context).stream().forEach(searchResult -> matches.add(searchResult.getLabel() + AttributeMatcher.SEPARATOR + searchResult.getMatch()));
            } catch (final Throwable t) {
                LOGGER.error("Error happened during searchable matching: {}", t.getMessage());
                t.printStackTrace();
            }
        }
    }

    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    public void setVariableRegistry(final VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }
}
