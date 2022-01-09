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

package org.apache.nifi.registry.flow.diff;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StandardFlowComparison implements FlowComparison {

    private final ComparableDataFlow flowA;
    private final ComparableDataFlow flowB;
    private final Set<FlowDifference> differences;

    public StandardFlowComparison(final ComparableDataFlow flowA, final ComparableDataFlow flowB) {
        this.flowA = flowA;
        this.flowB = flowB;
        this.differences = new HashSet<>();
    }

    public StandardFlowComparison(final ComparableDataFlow flowA, final ComparableDataFlow flowB, final Set<FlowDifference> differences) {
        this.flowA = flowA;
        this.flowB = flowB;
        this.differences = differences;
    }

    @Override
    public ComparableDataFlow getFlowA() {
        return flowA;
    }

    @Override
    public ComparableDataFlow getFlowB() {
        return flowB;
    }

    @Override
    public Set<FlowDifference> getDifferences() {
        return Collections.unmodifiableSet(differences);
    }

    public void addDifference(final FlowDifference difference) {
        this.differences.add(difference);
    }
}
