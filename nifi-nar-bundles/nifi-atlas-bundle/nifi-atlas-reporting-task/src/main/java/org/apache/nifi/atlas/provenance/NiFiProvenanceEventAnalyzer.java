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

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

/**
 * Responsible for analyzing NiFi provenance event data to generate Atlas DataSet reference.
 * Implementations of this interface should be thread safe.
 */
public interface NiFiProvenanceEventAnalyzer {

    DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event);

    /**
     * Returns target component type pattern that this Analyzer supports.
     * Note that a component type of NiFi provenance event only has processor type name without package name.
     * @return A RegularExpression to match with a component type of a provenance event.
     */
    default String targetComponentTypePattern() {
        return null;
    }

    /**
     * Returns target transit URI pattern that this Analyzer supports.
     * @return A RegularExpression to match with a transit URI of a provenance event.
     */
    default String targetTransitUriPattern() {
        return null;
    }

    /**
     * Returns target provenance event type that this Analyzer supports.
     * @return A Provenance event type
     */
    default ProvenanceEventType targetProvenanceEventType() {
        return null;
    }

}
