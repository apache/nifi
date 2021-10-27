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
package org.apache.nifi.rules.engine;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.rules.Action;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockRulesEngineService extends AbstractConfigurableComponent implements RulesEngineService {
    private final List<Action> actions;

    public MockRulesEngineService(List<Action> actions) {
        this.actions = actions;
    }

    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        // check if it's the unique bulletin query
        if (facts.containsKey("bulletinCategory")) {
            final String bulletinCategory = (String) facts.get("bulletinCategory");
            if (bulletinCategory.equals("processor")) {
                return Collections.singletonList(actions.get(0));
            } else if (bulletinCategory.equals("controller service")) {
                return Collections.singletonList(actions.get(1));
            }
        }
        // check if it's the unique provenance query
        if (facts.containsKey("componentId")) {
            final String bulletinCategory = (String) facts.get("componentId");
            if (bulletinCategory.equals("1")) {
                return Collections.singletonList(actions.get(0));
            } else if (bulletinCategory.equals("2")) {
                return Collections.singletonList(actions.get(1));
            }
        }
        return actions;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) {
    }

    @Override
    public String getIdentifier() {
        return "MockRulesEngineService";
    }
}
