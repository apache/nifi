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
package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class DataSetRefs {
    private final Set<String> componentIds;
    private Set<Referenceable> inputs;
    private Set<Referenceable> outputs;
    private boolean referableFromRootPath;

    public DataSetRefs(String componentId) {
        this.componentIds = Collections.singleton(componentId);
    }

    public DataSetRefs(Set<String> componentIds) {
        this.componentIds = componentIds;
    }

    public Set<String> getComponentIds() {
        return componentIds;
    }

    public Set<Referenceable> getInputs() {
        return inputs != null ? inputs : Collections.emptySet();
    }

    public void addInput(Referenceable input) {
        if (inputs == null) {
            inputs = new LinkedHashSet<>();
        }
        inputs.add(input);
    }

    public Set<Referenceable> getOutputs() {
        return outputs != null ? outputs : Collections.emptySet();
    }

    public void addOutput(Referenceable output) {
        if (outputs == null) {
            outputs = new LinkedHashSet<>();
        }
        outputs.add(output);
    }

    public boolean isEmpty() {
        return (inputs == null || inputs.isEmpty()) && (outputs == null || outputs.isEmpty());
    }

}
