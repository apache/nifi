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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.StatelessDataflow;

public interface StatelessEngine<T> {

    void initialize(StatelessEngineInitializationContext initializationContext);

    StatelessDataflow createFlow(DataflowDefinition<T> dataflowDefinition);

    ExtensionManager getExtensionManager();

    BulletinRepository getBulletinRepository();

    StateManagerProvider getStateManagerProvider();

    PropertyEncryptor getPropertyEncryptor();

    FlowRegistryClient getFlowRegistryClient();

    FlowManager getFlowManager();

    VariableRegistry getRootVariableRegistry();

    ProcessScheduler getProcessScheduler();

    ReloadComponent getReloadComponent();

    ControllerServiceProvider getControllerServiceProvider();

    KerberosConfig getKerberosConfig();

    ValidationTrigger getValidationTrigger();

    ProvenanceRepository getProvenanceRepository();

    FlowFileEventRepository getFlowFileEventRepository();

    CounterRepository getCounterRepository();
}