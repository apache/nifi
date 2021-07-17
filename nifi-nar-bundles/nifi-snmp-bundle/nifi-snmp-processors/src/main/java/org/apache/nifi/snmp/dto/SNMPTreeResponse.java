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
package org.apache.nifi.snmp.dto;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.Target;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SNMPTreeResponse {

    private final Target target;
    private final List<TreeEvent> events;

    public SNMPTreeResponse(final Target target, final List<TreeEvent> events) {
        this.target = target;
        this.events = events;
    }

    public Map<String, String> getAttributes() {
        final List<VariableBinding> variableBindings = events.stream()
                .map(TreeEvent::getVariableBindings)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
        return SNMPUtils.createWalkOidValuesMap(variableBindings);
    }

    public String getTargetAddress() {
        return target.getAddress().toString();
    }

    public void logErrors(final ComponentLog logger) {
        events.stream()
                .filter(TreeEvent::isError)
                .forEach(event -> logger.error("Error occured in SNMP walk event: {}", event.getErrorMessage()));
    }
}
