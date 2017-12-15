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

import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class NiFiProvenanceEventAnalyzerFactory {

    /**
     * This holder class is used to implement initialization-on-demand holder idiom to avoid double-checked locking anti-pattern.
     * The static initializer is performed only once for a class loader.
     * See these links for detail:
     * <ul>
     *     <li><a href="https://en.wikipedia.org/wiki/Double-checked_locking">Double-checked locking</a></li>
     *     <li><a href="https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom">Initialization-on-demand holder</a></li>
     * </ul>
     */
    private static class AnalyzerHolder {
        private static final Logger logger = LoggerFactory.getLogger(NiFiProvenanceEventAnalyzerFactory.AnalyzerHolder.class);
        private static final Map<Pattern, NiFiProvenanceEventAnalyzer> analyzersForComponentType = new ConcurrentHashMap<>();
        private static final Map<Pattern, NiFiProvenanceEventAnalyzer> analyzersForTransitUri = new ConcurrentHashMap<>();
        private static final Map<ProvenanceEventType, NiFiProvenanceEventAnalyzer> analyzersForProvenanceEventType = new ConcurrentHashMap<>();

        private static void addAnalyzer(String patternStr, Map<Pattern, NiFiProvenanceEventAnalyzer> toAdd,
                                        NiFiProvenanceEventAnalyzer analyzer) {
            if (patternStr != null && !patternStr.isEmpty()) {
                Pattern pattern = Pattern.compile(patternStr.trim());
                toAdd.put(pattern, analyzer);
            }
        }

        static {
            logger.debug("Loading NiFiProvenanceEventAnalyzer ...");
            final ServiceLoader<NiFiProvenanceEventAnalyzer> serviceLoader
                    = ServiceLoader.load(NiFiProvenanceEventAnalyzer.class);
            serviceLoader.forEach(analyzer -> {
                addAnalyzer(analyzer.targetComponentTypePattern(), analyzersForComponentType, analyzer);
                addAnalyzer(analyzer.targetTransitUriPattern(), analyzersForTransitUri, analyzer);
                final ProvenanceEventType eventType = analyzer.targetProvenanceEventType();
                if (eventType != null) {
                    if (analyzersForProvenanceEventType.containsKey(eventType)) {
                        logger.warn("Fo ProvenanceEventType {}, an Analyzer {} is already assigned." +
                                        " Only one analyzer for a type can be registered. Ignoring {}",
                                eventType, analyzersForProvenanceEventType.get(eventType), analyzer);
                    }
                    analyzersForProvenanceEventType.put(eventType, analyzer);
                }
            });
            logger.info("Loaded NiFiProvenanceEventAnalyzers: componentTypes={}, transitUris={}", analyzersForComponentType, analyzersForTransitUri);
        }

        private static Map<Pattern, NiFiProvenanceEventAnalyzer> getAnalyzersForComponentType() {
            return analyzersForComponentType;
        }

        private static Map<Pattern, NiFiProvenanceEventAnalyzer> getAnalyzersForTransitUri() {
            return analyzersForTransitUri;
        }

        private static Map<ProvenanceEventType, NiFiProvenanceEventAnalyzer> getAnalyzersForProvenanceEventType() {
            return analyzersForProvenanceEventType;
        }
    }


    /**
     * Find and retrieve NiFiProvenanceEventAnalyzer implementation for the specified targets.
     * Pattern matching is performed by following order, and the one found at first is returned:
     * <ol>
     * <li>Component type name. Use an analyzer supporting the Component type with its {@link NiFiProvenanceEventAnalyzer#targetProvenanceEventType()}.
     * <li>TransitUri. Use an analyzer supporting the TransitUri with its {@link NiFiProvenanceEventAnalyzer#targetTransitUriPattern()}.
     * <li>Provenance Event Type. Use an analyzer supporting the Provenance Event Type with its {@link NiFiProvenanceEventAnalyzer#targetProvenanceEventType()}.
     * </ol>
     * @param typeName NiFi component type name.
     * @param transitUri Transit URI.
     * @param eventType Provenance event type.
     * @return Instance of NiFiProvenanceEventAnalyzer if one is found for the specified className, otherwise null.
     */
    public static NiFiProvenanceEventAnalyzer getAnalyzer(String typeName, String transitUri, ProvenanceEventType eventType) {

        for (Map.Entry<Pattern, NiFiProvenanceEventAnalyzer> entry
                : NiFiProvenanceEventAnalyzerFactory.AnalyzerHolder.getAnalyzersForComponentType().entrySet()) {
            if (entry.getKey().matcher(typeName).matches()) {
                return entry.getValue();
            }
        }

        if (transitUri != null) {
            for (Map.Entry<Pattern, NiFiProvenanceEventAnalyzer> entry
                    : NiFiProvenanceEventAnalyzerFactory.AnalyzerHolder.getAnalyzersForTransitUri().entrySet()) {
                if (entry.getKey().matcher(transitUri).matches()) {
                    return entry.getValue();
                }
            }
        }

        // If there's no specific implementation, just use generic analyzer.
        return NiFiProvenanceEventAnalyzerFactory.AnalyzerHolder.getAnalyzersForProvenanceEventType().get(eventType);
    }
}
