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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.provenance.ProvenanceEventRepository;

public interface DataflowTriggerContext {

    /**
     * Provides a mechanism by which the triggering class can abort a dataflow
     * @return <code>true</code> if the dataflow should be aborted, <code>false</code> otherwise
     */
    boolean isAbort();

    default FlowFileSupplier getFlowFileSupplier() {
        return null;
    }

    ProvenanceEventRepository getProvenanceEventRepository();

    /**
     * The implicit context that will be used if no other context is provided when triggering a dataflow
     */
    DataflowTriggerContext IMPLICIT_CONTEXT = new DataflowTriggerContext() {
        private final ProvenanceEventRepository eventRepo = new NopProvenanceEventRepository();

        @Override
        public boolean isAbort() {
            return false;
        }

        @Override
        public ProvenanceEventRepository getProvenanceEventRepository() {
            return eventRepo;
        }
    };
}
