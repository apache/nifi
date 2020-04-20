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
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.toStr;
import static org.apache.nifi.atlas.AtlasUtils.toTypedQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.hook.NiFiAtlasHook.NIFI_USER;

public abstract class AbstractLineageStrategy implements LineageStrategy {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    private LineageContext lineageContext;

    public void setLineageContext(LineageContext lineageContext) {
        this.lineageContext = lineageContext;
    }

    protected DataSetRefs executeAnalyzer(AnalysisContext analysisContext, ProvenanceEventRecord event) {
        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(event.getComponentType(), event.getTransitUri(), event.getEventType());
        if (analyzer == null) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzer {} is found for event: {}", analyzer, event);
        }
        return analyzer.analyze(analysisContext, event);
    }

    protected void addDataSetRefs(NiFiFlow nifiFlow, DataSetRefs refs) {

        final Set<NiFiFlowPath> flowPaths = refs.getComponentIds().stream()
                .map(componentId -> {
                    final NiFiFlowPath flowPath = nifiFlow.findPath(componentId);
                    if (flowPath == null) {
                        logger.warn("FlowPath for {} was not found.", componentId);
                    }
                    return flowPath;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        addDataSetRefs(nifiFlow, flowPaths, refs);
    }

    protected void addDataSetRefs(NiFiFlow nifiFlow, Set<NiFiFlowPath> flowPaths, DataSetRefs refs) {
        // create reference to NiFi flow path.
        final Referenceable flowRef = toReferenceable(nifiFlow);
        final String namespace = nifiFlow.getNamespace();
        final String url = nifiFlow.getUrl();

        for (NiFiFlowPath flowPath : flowPaths) {
            final Referenceable flowPathRef = toReferenceable(flowPath, flowRef, namespace, url);
            addDataSetRefs(refs, flowPathRef);
        }
    }

    private Referenceable toReferenceable(NiFiFlow nifiFlow) {
        final Referenceable flowRef = new Referenceable(TYPE_NIFI_FLOW);
        flowRef.set(ATTR_NAME, nifiFlow.getFlowName());
        flowRef.set(ATTR_QUALIFIED_NAME, nifiFlow.getQualifiedName());
        flowRef.set(ATTR_URL, nifiFlow.getUrl());
        return flowRef;
    }

    protected Referenceable toReferenceable(NiFiFlowPath flowPath, NiFiFlow nifiFlow) {
        return toReferenceable(flowPath, toReferenceable(nifiFlow),
                nifiFlow.getNamespace(), nifiFlow.getUrl());
    }

    private Referenceable toReferenceable(NiFiFlowPath flowPath, Referenceable flowRef, String namespace, String nifiUrl) {
        final Referenceable flowPathRef = new Referenceable(TYPE_NIFI_FLOW_PATH);
        flowPathRef.set(ATTR_NAME, flowPath.getName());
        flowPathRef.set(ATTR_QUALIFIED_NAME, flowPath.getId() + "@" + namespace);
        flowPathRef.set(ATTR_NIFI_FLOW, flowRef);
        flowPathRef.set(ATTR_URL, flowPath.createDeepLinkURL(nifiUrl));
        // Referenceable has to have GUID assigned, otherwise it will not be stored due to lack of required attribute.
        // If a Referencible has GUID, Atlas does not validate all required attributes.
        flowPathRef.set(ATTR_INPUTS, flowPath.getInputs().stream().map(this::toReferenceable).collect(Collectors.toList()));
        flowPathRef.set(ATTR_OUTPUTS,  flowPath.getOutputs().stream().map(this::toReferenceable).collect(Collectors.toList()));
        return flowPathRef;
    }

    private Referenceable toReferenceable(AtlasObjectId id) {
        return StringUtils.isEmpty(id.getGuid())
                ? new Referenceable(id.getTypeName(), id.getUniqueAttributes())
                : new Referenceable(id.getGuid(), id.getTypeName(), id.getUniqueAttributes());
    }

    protected void createEntity(Referenceable ... entities) {
        final HookNotificationV1.EntityCreateRequest msg = new HookNotificationV1.EntityCreateRequest(NIFI_USER, entities);
        lineageContext.addMessage(msg);
    }

    @SuppressWarnings("unchecked")
    protected boolean addDataSetRefs(Set<Referenceable> refsToAdd, Referenceable nifiFlowPath, String targetAttribute) {
        if (refsToAdd != null && !refsToAdd.isEmpty()) {

            // If nifiFlowPath already has a given dataSetRef, then it needs not to be created.
            final Function<Referenceable, String> toTypedQualifiedName = ref -> toTypedQualifiedName(ref.getTypeName(), toStr(ref.get(ATTR_QUALIFIED_NAME)));
            final Collection<Referenceable> refs = Optional.ofNullable((Collection<Referenceable>) nifiFlowPath.get(targetAttribute)).orElseGet(ArrayList::new);
            final Set<String> existingRefTypedQualifiedNames = refs.stream().map(toTypedQualifiedName).collect(Collectors.toSet());

            refsToAdd.stream().filter(ref -> !existingRefTypedQualifiedNames.contains(toTypedQualifiedName.apply(ref)))
                    .forEach(ref -> {
                        if (isUnassigned(ref.getId())) {
                            // Create new entity.
                            logger.debug("Found a new DataSet reference from {} to {}, sending an EntityCreateRequest",
                                    new Object[]{toTypedQualifiedName.apply(nifiFlowPath), toTypedQualifiedName.apply(ref)});
                            final HookNotificationV1.EntityCreateRequest createDataSet = new HookNotificationV1.EntityCreateRequest(NIFI_USER, ref);
                            lineageContext.addMessage(createDataSet);
                        }
                        refs.add(ref);
                    });

            if (refs.size() > existingRefTypedQualifiedNames.size()) {
                // Something has been added.
                nifiFlowPath.set(targetAttribute, refs);
                return true;
            }
        }
        return false;
    }

    protected void addDataSetRefs(DataSetRefs dataSetRefs, Referenceable flowPathRef) {
        final boolean inputsAdded = addDataSetRefs(dataSetRefs.getInputs(), flowPathRef, ATTR_INPUTS);
        final boolean outputsAdded = addDataSetRefs(dataSetRefs.getOutputs(), flowPathRef, ATTR_OUTPUTS);
        if (inputsAdded || outputsAdded) {
            lineageContext.addMessage(new HookNotificationV1.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                    ATTR_QUALIFIED_NAME, (String) flowPathRef.get(ATTR_QUALIFIED_NAME), flowPathRef));
        }
    }

    // Copy of org.apache.atlas.typesystem.persistence.Id.isUnassigned() from v0.8.1. This method does not exists in v2.0.0.
    private boolean isUnassigned(Id id) {
        try {
            long l = Long.parseLong(id.getId());
            return l < 0;
        } catch (NumberFormatException ne) {
            return false;
        }
    }
}
