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

package org.apache.nifi.minifi.commons.service;

import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.concat;

public class StandardFlowPropertyAssetReferenceResolverService implements FlowPropertyAssetReferenceResolver {

    private static final String ASSET_REFERENCE_PREFIX = "@{asset-id:";
    private static final String ASSET_REFERENCE_SUFFIX = "}";
    private static final String EMPTY_STRING = "";

    private final Function<String, Optional<Path>> assetPathResolver;

    public StandardFlowPropertyAssetReferenceResolverService(Function<String, Optional<Path>> assetPathResolver) {
        this.assetPathResolver = assetPathResolver;
    }

    @Override
    public void resolveAssetReferenceProperties(VersionedDataflow flow) {
        fetchFlowComponents(flow).forEach(component -> {
            component.getProperties().entrySet().stream()
                .filter(e -> isAssetReference(e.getValue()))
                .forEach(entry -> entry.setValue(getAssetAbsolutePathOrThrowIllegalStateException(entry.getValue())));
        });
    }

    private boolean isAssetReference(String value) {
        return value != null
                && value.startsWith(ASSET_REFERENCE_PREFIX)
                && value.endsWith(ASSET_REFERENCE_SUFFIX);
    }

    private Stream<? extends VersionedConfigurableExtension> fetchFlowComponents(VersionedDataflow flow) {
        return concat(
                ofNullable(flow.getControllerServices()).orElse(List.of()).stream(),
                fetchComponentsRecursively(flow.getRootGroup())
        );
    }

    private Stream<? extends VersionedConfigurableExtension> fetchComponentsRecursively(VersionedProcessGroup processGroup) {
        return concat(
                Stream.of(
                        ofNullable(processGroup.getProcessors()).orElse(Set.of()),
                        ofNullable(processGroup.getControllerServices()).orElse(Set.of())
                        )
                .flatMap(Set::stream),
                ofNullable(processGroup.getProcessGroups()).orElse(Set.of()).stream()
                        .flatMap(this::fetchComponentsRecursively)
        );
    }

    private String getAssetAbsolutePathOrThrowIllegalStateException(String assetReference) {
        String resourceId = assetReference.replace(ASSET_REFERENCE_PREFIX, EMPTY_STRING)
                .replace(ASSET_REFERENCE_SUFFIX, EMPTY_STRING);
        return assetPathResolver.apply(resourceId)
                .map(Path::toString)
                .orElseThrow(() -> new IllegalStateException("Resource '" + resourceId + "' not found"));
    }
}
