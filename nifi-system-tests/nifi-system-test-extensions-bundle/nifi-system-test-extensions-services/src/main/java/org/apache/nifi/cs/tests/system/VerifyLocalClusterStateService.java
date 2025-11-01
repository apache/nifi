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

package org.apache.nifi.cs.tests.system;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class VerifyLocalClusterStateService extends AbstractControllerService implements SetStateService {

    static final PropertyDescriptor FILE_STORAGE_LOCATION = new PropertyDescriptor.Builder()
        .name("File Storage Location")
        .description("The file system location where state files will be written.")
        .addValidator(StandardValidators.createDirectoryExistsValidator(false, true))
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(FILE_STORAGE_LOCATION);
    }

    @OnEnabled
    public void onScheduled(final ConfigurationContext context) throws IOException {
        final String nodeNumber = System.getProperty("nodeNumber");
        final StateManager stateManager = getStateManager();
        final Map<String, String> clusterState = stateManager.getState(Scope.CLUSTER).toMap();
        final Map<String, String> localState = stateManager.getState(Scope.LOCAL).toMap();

        final File directory = new File(context.getProperty(FILE_STORAGE_LOCATION).getValue());

        writeToPropertiesFile(clusterState, new File(directory, "cluster-state-" + nodeNumber + ".properties"));
        writeToPropertiesFile(localState, new File(directory, "local-state-" + nodeNumber + ".properties"));
    }

    private void writeToPropertiesFile(final Map<String, String> stateMap, final File file) throws IOException {
        final Properties properties = new Properties();
        stateMap.forEach(properties::setProperty);
        try (final OutputStream out = new FileOutputStream(file)) {
            properties.store(out, "State Map");
        }
    }

    @Override
    public void setState(final String key, final String value, final Scope scope) throws IOException {
        final Map<String, String> state = new HashMap<>(getStateManager().getState(scope).toMap());
        state.put(key, value);
        getStateManager().setState(state, scope);
    }
}
