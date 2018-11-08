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
package org.apache.nifi.attribute.expression.language;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.registry.VariableRegistry;

import java.util.Map;

/**
 * A convenience class to encapsulate the logic of variable substitution
 * based first on any additional variable maps, then flow file properties,
 * then flow file attributes, and finally the provided variable registry.
 */
interface ValueLookup extends Map<String, String> {

    @SafeVarargs
    static ValueLookup of(VariableRegistry variableRegistry, FlowFile flowFile, Map<String, String>... additionalMaps) {
        if (additionalMaps.length == 0) {
            return new SingleValueLookup(variableRegistry, flowFile);
        } else if (flowFile == null && additionalMaps.length < 2) {
            if (additionalMaps.length == 0) {
                return new SingleValueLookup(variableRegistry, (FlowFile) null);
            } else {
                return new SingleValueLookup(variableRegistry, additionalMaps[0]);
            }
        } else {
            return new MultiValueLookup(variableRegistry, flowFile, additionalMaps);
        }
    }

}
