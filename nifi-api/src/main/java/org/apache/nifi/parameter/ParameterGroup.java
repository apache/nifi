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
package org.apache.nifi.parameter;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates a named group of externally fetched parameters that can be provided to referencing Parameter Contexts.
 */
public class ParameterGroup {

    private final String groupName;

    private final List<Parameter> parameters;

    /**
     * Creates a named parameter group.
     * @param groupName The parameter group name
     * @param parameters A list of parameters
     */
    public ParameterGroup(final String groupName, final List<Parameter> parameters) {
        this.groupName = groupName;
        this.parameters = Collections.unmodifiableList(parameters);
    }

    /**
     * @return The group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * @return The provided parameters
     */
    public List<Parameter> getParameters() {
        return parameters;
    }
}
