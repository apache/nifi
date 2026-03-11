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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class RebaseEngine {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final Map<DifferenceType, RebaseHandler> handlerRegistry;

    public RebaseEngine() {
        this.handlerRegistry = new HashMap<>();
        registerHandler(new PositionChangedRebaseHandler());
        registerHandler(new SizeChangedRebaseHandler());
        registerHandler(new BendpointsChangedRebaseHandler());
        registerHandler(new PropertyChangedRebaseHandler());
        registerHandler(new PropertyAddedRebaseHandler());
        registerHandler(new CommentsChangedRebaseHandler());
    }

    public RebaseEngine(final Map<DifferenceType, RebaseHandler> handlerRegistry) {
        this.handlerRegistry = new HashMap<>(handlerRegistry);
    }

    public RebaseAnalysis analyze(final Set<FlowDifference> localDifferences, final Set<FlowDifference> upstreamDifferences,
                                  final VersionedProcessGroup targetSnapshot) {
        final List<RebaseAnalysis.ClassifiedDifference> classifiedChanges = new ArrayList<>();
        boolean allCompatible = true;

        for (final FlowDifference localDifference : localDifferences) {
            final RebaseHandler handler = handlerRegistry.get(localDifference.getDifferenceType());
            if (handler == null) {
                classifiedChanges.add(RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, "NO_HANDLER",
                        "No rebase handler registered for difference type: " + localDifference.getDifferenceType().getDescription()));
                allCompatible = false;
                continue;
            }

            final RebaseAnalysis.ClassifiedDifference classified = handler.classify(localDifference, upstreamDifferences, targetSnapshot);
            classifiedChanges.add(classified);
            if (classified.getClassification() != RebaseClassification.COMPATIBLE) {
                allCompatible = false;
            }
        }

        VersionedProcessGroup mergedSnapshot = null;
        if (allCompatible) {
            mergedSnapshot = deepClone(targetSnapshot);
            for (final RebaseAnalysis.ClassifiedDifference classified : classifiedChanges) {
                final RebaseHandler handler = handlerRegistry.get(classified.getDifference().getDifferenceType());
                if (handler != null) {
                    handler.apply(classified.getDifference(), mergedSnapshot);
                }
            }
        }

        final String fingerprint = computeAnalysisFingerprint(classifiedChanges, upstreamDifferences);
        return new RebaseAnalysis(classifiedChanges, upstreamDifferences, allCompatible, fingerprint, mergedSnapshot);
    }

    public static String computeConflictKey(final FlowDifference difference) {
        final DifferenceType type = difference.getDifferenceType();
        final String componentId = resolveComponentIdentifier(difference);
        final Optional<String> fieldName = difference.getFieldName();

        return switch (type) {
            case PROPERTY_CHANGED -> type.name() + ":" + componentId + ":" + fieldName.orElse("");
            case POSITION_CHANGED, SIZE_CHANGED, COMMENTS_CHANGED, DESCRIPTION_CHANGED -> type.name() + ":" + componentId;
            case BENDPOINTS_CHANGED -> type.name() + ":" + componentId;
            default -> type.name() + ":" + componentId + ":" + fieldName.orElse("");
        };
    }

    VersionedProcessGroup deepClone(final VersionedProcessGroup source) {
        try {
            final byte[] serialized = OBJECT_MAPPER.writeValueAsBytes(source);
            return OBJECT_MAPPER.readValue(serialized, VersionedProcessGroup.class);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to deep clone VersionedProcessGroup", e);
        }
    }

    private static String resolveComponentIdentifier(final FlowDifference difference) {
        if (difference.getComponentB() != null) {
            return difference.getComponentB().getIdentifier();
        }
        if (difference.getComponentA() != null) {
            return difference.getComponentA().getIdentifier();
        }
        return "";
    }

    private String computeAnalysisFingerprint(final List<RebaseAnalysis.ClassifiedDifference> classifiedChanges,
                                              final Set<FlowDifference> upstreamDifferences) {
        final SortedSet<String> sortedKeys = new TreeSet<>();

        for (final RebaseAnalysis.ClassifiedDifference classified : classifiedChanges) {
            final String key = computeConflictKey(classified.getDifference());
            sortedKeys.add("local:" + key + ":" + classified.getClassification().name());
        }

        for (final FlowDifference upstream : upstreamDifferences) {
            final String key = computeConflictKey(upstream);
            sortedKeys.add("upstream:" + key);
        }

        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (final String key : sortedKeys) {
                digest.update(key.getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    private void registerHandler(final RebaseHandler handler) {
        handlerRegistry.put(handler.getSupportedType(), handler);
    }

    private static ObjectMapper createObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}
