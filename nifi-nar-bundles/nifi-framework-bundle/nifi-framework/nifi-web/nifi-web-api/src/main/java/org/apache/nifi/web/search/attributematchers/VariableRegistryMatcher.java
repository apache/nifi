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

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.web.search.attributematchers.AttributeMatcher.addIfMatching;

public class VariableRegistryMatcher implements AttributeMatcher<ProcessGroup> {
    private static final String LABEL_NAME = "Variable Name";
    private static final String LABEL_VALUE = "Variable Value";

    @Override
    public void match(final ProcessGroup component, final SearchQuery query, final List<String> matches) {
        final ComponentVariableRegistry variableRegistry = component.getVariableRegistry();

        if (variableRegistry != null) {
            for (final Map.Entry<VariableDescriptor, String> entry : variableRegistry.getVariableMap().entrySet()) {
                addIfMatching(query.getTerm(), entry.getKey().getName(), LABEL_NAME, matches);
                addIfMatching(query.getTerm(), entry.getValue(), LABEL_VALUE, matches);
            }
        }
    }
}
