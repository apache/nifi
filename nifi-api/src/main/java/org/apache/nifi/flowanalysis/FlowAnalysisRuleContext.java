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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.VersionedControllerServiceLookup;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;

import java.util.Map;

/**
 * This interface provides a bridge between the NiFi Framework and a
 * {@link FlowAnalysisRule}. This context allows a FlowAnalysisRule to access
 * configuration supplied by the user.
 */
public interface FlowAnalysisRuleContext extends PropertyContext {
    /**
     * @return the name of the rule that is being triggered
     */
    String getRuleName();

    /**
     * @return a Map of all known {@link PropertyDescriptor}s to their
     * configured properties. This Map will contain a <code>null</code> for any
     * Property that has not been configured by the user, even if the
     * PropertyDescriptor has a default value
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * @return the {@link BulletinRepository} that can be used to analyze
     * Bulletins that have been emitted and register new Bulletins
     */
    BulletinRepository getBulletinRepository();

    /**
     * Creates a controller-level {@link Bulletin} with the given category, severity
     * level, and message, so that the Bulletin can be added to the
     * {@link BulletinRepository}. Access to this bulletin will be enforce through
     * permissions on the controller.
     *
     * @param category of bulletin
     * @param severity of bulletin
     * @param message of bulletin
     * @return new bulletin
     */
    Bulletin createBulletin(String category, Severity severity, String message);

    /**
     * Creates a {@link Bulletin} for the component with the specified
     * identifier.
     *
     * @param componentId the ID of the component
     * @param category the name of the bulletin's category
     * @param severity the severity level of the bulletin
     * @param message the bulletin's message
     * @return new bulletin
     */
    Bulletin createBulletin(String componentId, String category, Severity severity, String message);

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * @return the {@link VersionedControllerServiceLookup} which can be used to obtain
     * Versioned Controller Services during flow analysis
     */
    VersionedControllerServiceLookup getVersionedControllerServiceLookup();

    /**
     * @return the StateManager that can be used to store and retrieve state for this component
     */
    StateManager getStateManager();

    /**
     * @return <code>true</code> if this instance of NiFi is configured to be part of a cluster, <code>false</code>
     *         if this instance of NiFi is a standalone instance
     */
    boolean isClustered();

    /**
     * @return the ID of this node in the cluster, or <code>null</code> if either this node is not clustered or the Node Identifier
     *         has not yet been established
     */
    String getClusterNodeIdentifier();
}
