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
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockRulesEngineService extends AbstractConfigurableComponent implements RulesEngineService {
    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        return Collections.singletonList(Mockito.mock(Action.class));
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) {
    }

    @Override
    public String getIdentifier() {
        return "MockRulesEngineService";
    }
}
