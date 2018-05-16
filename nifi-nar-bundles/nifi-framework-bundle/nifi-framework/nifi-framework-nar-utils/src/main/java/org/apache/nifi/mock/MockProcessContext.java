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
package org.apache.nifi.mock;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MockProcessContext implements ProcessContext {

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        return null;
    }

    @Override
    public PropertyValue getProperty(String propertyName) {
        return null;
    }

    @Override
    public PropertyValue newPropertyValue(String rawValue) {
        return null;
    }

    @Override
    public void yield() {

    }

    @Override
    public int getMaxConcurrentTasks() {
        return 0;
    }

    @Override
    public String getAnnotationData() {
        return "";
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getAllProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String encrypt(String unencrypted) {
        return unencrypted;
    }

    @Override
    public String decrypt(String encrypted) {
        return encrypted;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return new MockControllerServiceLookup();
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        return Collections.emptySet();
    }

    @Override
    public boolean hasIncomingConnection() {
        return true;
    }

    @Override
    public boolean hasNonLoopConnection() {
        return true;
    }

    @Override
    public boolean hasConnection(Relationship relationship) {
        return false;
    }

    @Override
    public boolean isExpressionLanguagePresent(PropertyDescriptor property) {
        return false;
    }

    @Override
    public StateManager getStateManager() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }
}