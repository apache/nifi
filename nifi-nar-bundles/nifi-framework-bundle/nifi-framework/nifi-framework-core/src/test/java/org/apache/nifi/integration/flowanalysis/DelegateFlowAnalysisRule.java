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
package org.apache.nifi.integration.flowanalysis;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DelegateFlowAnalysisRule extends AbstractFlowAnalysisRule {
    private FlowAnalysisRule delegate;

    public DelegateFlowAnalysisRule() {
    }

    @Override
    public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
        return delegate.analyzeComponent(component, context);
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
        return delegate.analyzeProcessGroup(processGroup, context);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (delegate == null) {
            return Collections.emptyList();
        } else {
            return delegate.getPropertyDescriptors();
        }
    }

    public FlowAnalysisRule getDelegate() {
        return delegate;
    }

    public void setDelegate(FlowAnalysisRule delegate) {
        this.delegate = delegate;
    }
}
