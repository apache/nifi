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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.nar.ExtensionManager;

import java.util.Set;

/**
 * A FlowAnalysisRuleProvider is responsible for providing management of, and
 * access to, Flow Analysis Rules
 */
public interface FlowAnalysisRuleProvider {

    /**
     * Creates a new instance of a flow analysis rule
     *
     * @param type the type (fully qualified class name) of the flow analysis rule
     * to instantiate
     * @param id the identifier for the Flow Analysis Rule
     * @param bundleCoordinate the bundle coordinate for the type of flow analysis rule
     * @param firstTimeAdded whether or not this is the first time that the
     * flow analysis rule is being added to the flow. I.e., this will be true only
     * when the user adds the flow analysis rule to the flow, not when the flow is
     * being restored after a restart of the software
     *
     * @return the FlowAnalysisRuleNode that is used to manage the flow analysis rule
     *
     * @throws FlowAnalysisRuleInstantiationException if unable to create the
     * Flow Analysis Rule
     */
    FlowAnalysisRuleNode createFlowAnalysisRule(String type, String id, BundleCoordinate bundleCoordinate, boolean firstTimeAdded) throws FlowAnalysisRuleInstantiationException;

    /**
     * @param identifier of node
     * @return the flow analysis rule that has the given identifier, or
     * <code>null</code> if no flow analysis rule exists with that ID
     */
    FlowAnalysisRuleNode getFlowAnalysisRuleNode(String identifier);

    /**
     * @return a Set of all Flow Analysis Rules that exist for this service
     * provider
     */
    Set<FlowAnalysisRuleNode> getAllFlowAnalysisRules();

    /**
     * Removes the given flow analysis rule from the flow
     *
     * @param flowAnalysisRule to remove
     *
     * @throws IllegalStateException if the flow analysis rule cannot be removed
     * because it is not disabled, or if the flow analysis rule is not known in the
     * flow
     */
    void removeFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule);

    /**
     * Enables the flow analysis rule
     *
     * @param flowAnalysisRule to enable
     *
     * @throws IllegalStateException if the FlowAnalysisRule's state is not
     * DISABLED
     */
    void enableFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule);

    /**
     * Disables the flow analysis rule
     *
     * @param flowAnalysisRule to disable
     *
     * @throws IllegalStateException if the FlowAnalysisRule's state is not
     * ENABLED
     */
    void disableFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule);

    /**
     * @return the ExtensionManager instance used by this provider
     */
    ExtensionManager getExtensionManager();

}
