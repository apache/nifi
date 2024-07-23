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
package org.apache.nifi.web.util;

import java.util.Collection;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Replaces Parameter Contexts within the snapshot following name conventions.
 *
 * A set of Parameter Contexts following a given name convention is considered as a lineage. Lineage is used to
 * group Parameter Contexts and determine a non-conflicting name for the newly created replacements. This class
 * creates replaces all Parameter Contexts in the snapshot, keeping care of the lineage for the given Contexts.
 *
 * Note: if multiple (sub)groups refer to the same Parameter Context, only one replacement will be created and all
 * Process Groups referred to the original Parameter Context will refer to this replacement.
 */
public class ParameterContextReplacer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParameterContextReplacer.class);

    private final ParameterContextNameCollisionResolver nameCollisionResolver;

    public ParameterContextReplacer(final ParameterContextNameCollisionResolver nameCollisionResolver) {
        this.nameCollisionResolver = nameCollisionResolver;
    }

    /**
     * Goes through the Process Group structure and replaces Parameter Contexts to avoid collision with the ones
     * existing in the flow, based on name. The method disregards if the given Parameter Context has no matching
     * counterpart in the existing flow, it replaces all with newly created contexts.
     *
     * @param flowSnapshot Snapshot from the Registry. Modification will be applied on this object!
     */
    public void replaceParameterContexts(final RegisteredFlowSnapshot flowSnapshot, final Collection<ParameterContextEntity> existingContexts) {
        // We do not want to have double replacements: within the snapshot we keep the identical names identical.
        final Map<String, VersionedParameterContext> parameterContexts = flowSnapshot.getParameterContexts();
        final Map<String, VersionedParameterContext> replacements = replaceParameterContexts(flowSnapshot.getFlowContents(), parameterContexts, new HashMap<>(), existingContexts);

        // This is needed because if a PC is used for both assignments and inheritance (parent) then we would both change it
        // but without updating the inheritance reference.  {@see NIFI-11706 TC#8}
        for (final Map.Entry<String, VersionedParameterContext> replacement : replacements.entrySet()) {
            for (final VersionedParameterContext parameterContext : parameterContexts.values()) {
                final List<String> inheritedContexts = parameterContext.getInheritedParameterContexts();
                if (inheritedContexts.contains(replacement.getKey())) {
                    inheritedContexts.remove(replacement.getKey());
                    inheritedContexts.add(replacement.getValue().getName());
                }
            }
        }
    }

    /**
     * @return A collection of replaced Parameter Contexts. Every map entry represents a singe replacement where the key
     * is the old context's name and the value is the new context.
     */
    private Map<String, VersionedParameterContext> replaceParameterContexts(
        final VersionedProcessGroup group,
        final Map<String, VersionedParameterContext> flowParameterContexts,
        final Map<String, VersionedParameterContext> replacements,
        final Collection<ParameterContextEntity> existingContexts
    ) {
        if (group.getParameterContextName() != null) {
            final String oldParameterContextName = group.getParameterContextName();
            final VersionedParameterContext oldParameterContext = flowParameterContexts.get(oldParameterContextName);

            if (replacements.containsKey(oldParameterContextName)) {
                final String replacementContextName = replacements.get(oldParameterContextName).getName();
                group.setParameterContextName(replacementContextName);
                LOGGER.debug("Replacing Parameter Context in Group {} from {} into {}", group.getIdentifier(), oldParameterContext, replacementContextName);
            } else {
                final VersionedParameterContext replacementContext = createReplacementContext(oldParameterContext, existingContexts);
                group.setParameterContextName(replacementContext.getName());

                flowParameterContexts.remove(oldParameterContextName);
                flowParameterContexts.put(replacementContext.getName(), replacementContext);
                replacements.put(oldParameterContextName, replacementContext);
                LOGGER.debug("Replacing Parameter Context in Group {} from {} into the newly created {}", group.getIdentifier(), oldParameterContext, replacementContext.getName());
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            replaceParameterContexts(childGroup, flowParameterContexts, replacements, existingContexts);
        }

        return replacements;
    }

    private VersionedParameterContext createReplacementContext(final VersionedParameterContext original, final Collection<ParameterContextEntity> existingContexts)  {
        final VersionedParameterContext replacement = new VersionedParameterContext();
        replacement.setName(nameCollisionResolver.resolveNameCollision(original.getName(), existingContexts));
        replacement.setParameters(new HashSet<>(original.getParameters()));
        replacement.setInheritedParameterContexts(Optional.ofNullable(original.getInheritedParameterContexts()).orElse(new ArrayList<>()));
        replacement.setDescription(original.getDescription());
        replacement.setSynchronized(original.isSynchronized());
        replacement.setParameterProvider(original.getParameterProvider());
        replacement.setParameterGroupName(original.getParameterGroupName());
        return replacement;
    }
}
