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
package org.apache.nifi.rules;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockPropertyContextActionHandler extends AbstractConfigurableComponent implements PropertyContextActionHandler{

    private List<Map<String, Object>> rows = new ArrayList<>();
    private List<Tuple<String,Action>> defaultActions = new ArrayList<>();
    private List<PropertyContext> propertyContexts = new ArrayList<>();


    @Override
    public void execute(PropertyContext context, Action action, Map<String, Object> facts) {
        propertyContexts.add(context);
        execute(action, facts);
    }

    @Override
    public void execute(Action action, Map<String, Object> facts) {
        rows.add(facts);
        defaultActions.add( new Tuple<>(action.getType(),action));
    }


    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public List<Tuple<String, Action>> getDefaultActions() {
        return defaultActions;
    }

    public List<Tuple<String,Action>> getDefaultActionsByType(final String type){
        return defaultActions.stream().filter(stringActionTuple -> stringActionTuple
                .getKey().equalsIgnoreCase(type)).collect(Collectors.toList());
    }

    public List<PropertyContext> getPropertyContexts() {
        return propertyContexts;
    }

    @Override
    public String getIdentifier() {
        return "MockPropertyContextActionHandler";
    }
}
