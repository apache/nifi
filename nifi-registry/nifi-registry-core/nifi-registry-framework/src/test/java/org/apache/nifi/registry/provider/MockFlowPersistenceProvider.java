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
package org.apache.nifi.registry.provider;

import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.flow.FlowPersistenceException;

import java.util.Map;

public class MockFlowPersistenceProvider implements FlowPersistenceProvider {

    private Map<String,String> properties;

    @Override
    public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        properties = configurationContext.getProperties();
    }

    @Override
    public void saveFlowContent(FlowSnapshotContext context, byte[] content) throws FlowPersistenceException {

    }

    @Override
    public byte[] getFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {
        return new byte[0];
    }

    @Override
    public void deleteAllFlowContent(String bucketId, String flowId) throws FlowPersistenceException {

    }

    @Override
    public void deleteFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {

    }

    public Map<String,String> getProperties() {
        return properties;
    }
}
