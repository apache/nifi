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
package org.apache.nifi.web.dao;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;

/**
 * Facade exposing DAO-level component lookups that include Process Groups managed by a Connector.
 *
 * <p>The standard {@link ProcessorDAO}, {@link ConnectionDAO}, {@link ProcessGroupDAO}, etc. intentionally hide
 * components that live inside a Connector's managed Process Group hierarchy unless the owning Connector is in
 * Troubleshooting mode. The handful of REST endpoints that legitimately need to inspect or operate on components
 * within a Connector-managed flow (the FlowFile queue endpoints under {@code /connectors/{id}/...} and the
 * Connector-aware Process Group flow endpoint, for example) obtain those components through this facade so the
 * standard DAO surface remains free of the "include connector managed" flag.</p>
 */
public interface ConnectorManagedComponentLookup {

    /**
     * Locate a Processor by identifier, including processors within Connector-managed Process Groups regardless of the
     * owning Connector's state.
     */
    ProcessorNode getProcessor(String id);

    /**
     * Locate an input Port by identifier, including ports within Connector-managed Process Groups regardless of the
     * owning Connector's state.
     */
    Port getInputPort(String id);

    /**
     * Locate an output Port by identifier, including ports within Connector-managed Process Groups regardless of the
     * owning Connector's state.
     */
    Port getOutputPort(String id);

    /**
     * Locate a Connection by identifier, including connections within Connector-managed Process Groups regardless of
     * the owning Connector's state.
     */
    Connection getConnection(String id);

    /**
     * Locate a Process Group by identifier, including Connector-managed Process Groups regardless of the owning
     * Connector's state.
     */
    ProcessGroup getProcessGroup(String id);

    /**
     * Locate a Remote Process Group by identifier, including remote process groups within Connector-managed Process
     * Groups regardless of the owning Connector's state.
     */
    RemoteProcessGroup getRemoteProcessGroup(String id);

    /**
     * Locate a Controller Service by identifier, including services within Connector-managed Process Groups regardless
     * of the owning Connector's state.
     */
    ControllerServiceNode getControllerService(String id);

    /**
     * Locate a Label by identifier, including labels within Connector-managed Process Groups regardless of the owning
     * Connector's state.
     */
    Label getLabel(String id);

    /**
     * Locate a Funnel by identifier, including funnels within Connector-managed Process Groups regardless of the
     * owning Connector's state.
     */
    Funnel getFunnel(String id);
}
