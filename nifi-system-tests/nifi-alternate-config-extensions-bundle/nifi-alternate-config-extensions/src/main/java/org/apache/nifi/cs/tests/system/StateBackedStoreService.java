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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CapabilityDescription("""
        Store whose contents live in the component's local state, so that they are subject to the framework's own state lifecycle.
        The store records when it was first created, and on every enable the password it is enabled with.
        Removing a Controller Service clears its state, so a service that was torn down and recreated comes back with a later creation timestamp.
        Comparing the creation timestamp therefore distinguishes a preserved service from a replaced one, even when the replacement has the same identifier.
        """)
@Stateful(scopes = Scope.LOCAL, description = "Holds the creation timestamp of the store and the password it was last enabled with.")
public class StateBackedStoreService extends AbstractControllerService implements StoreService {

    /**
     * Name under which the migrating processor passes the password. Only this service's own migration renames it, as
     * real services do for the secrets that processor migrations hand to them.
     */
    public static final String OBSOLETE_STORE_PASSWORD = "store-password";

    static final PropertyDescriptor STORE_NAME = new PropertyDescriptor.Builder()
            .name("Store Name")
            .required(true)
            .defaultValue("store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor STORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("Store Password")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final String CREATED_KEY = "created";
    private static final String PASSWORD_KEY = "password";

    private static final List<PropertyDescriptor> PROPERTIES = List.of(STORE_NAME, STORE_PASSWORD);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty(OBSOLETE_STORE_PASSWORD, STORE_PASSWORD.getName());
    }

    /**
     * Establishes the store on first enable and keeps its creation timestamp afterwards, so that the timestamp survives
     * the disable and re-enable cycle that a version change performs.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException {
        final StateManager stateManager = getStateManager();
        final Map<String, String> state = new HashMap<>(stateManager.getState(Scope.LOCAL).toMap());
        state.putIfAbsent(CREATED_KEY, String.valueOf(System.currentTimeMillis()));

        final String password = context.getProperty(STORE_PASSWORD).getValue();
        if (password == null) {
            state.remove(PASSWORD_KEY);
        } else {
            state.put(PASSWORD_KEY, password);
        }

        stateManager.setState(state, Scope.LOCAL);
    }

}
