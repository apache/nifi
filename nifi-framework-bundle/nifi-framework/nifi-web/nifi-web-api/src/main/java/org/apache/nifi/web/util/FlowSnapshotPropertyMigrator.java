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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.migration.ControllerServiceCreationDetails;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowRegistryUtils;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Performs a minimal, in-memory migration of Versioned flow snapshot properties using
 * component-provided migration rules. This is intended exclusively as a preflight step
 * to assist External Controller Service resolution during flow update/replace operations.
 *
 * It does not create any services and does not affect live components.
 */
public class FlowSnapshotPropertyMigrator {
    private static final Logger logger = LoggerFactory.getLogger(FlowSnapshotPropertyMigrator.class);

    private final NiFiServiceFacade serviceFacade;

    public FlowSnapshotPropertyMigrator(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void migrate(final FlowSnapshotContainer flowSnapshotContainer) {
        if (flowSnapshotContainer == null) {
            return;
        }

        final RegisteredFlowSnapshot topLevelSnapshot = flowSnapshotContainer.getFlowSnapshot();
        if (topLevelSnapshot == null || topLevelSnapshot.getFlowContents() == null) {
            return;
        }

        final VersionedProcessGroup root = topLevelSnapshot.getFlowContents();
        migrateProcessGroupPropertiesRecursively(root, flowSnapshotContainer);
    }

    private void migrateProcessGroupPropertiesRecursively(final VersionedProcessGroup group, final FlowSnapshotContainer container) {
        if (group == null) {
            return;
        }

        // Processors
        if (group.getProcessors() != null) {
            group.getProcessors().forEach(this::migrateComponentProperties);
        }

        // Controller Services
        if (group.getControllerServices() != null) {
            group.getControllerServices().forEach(this::migrateComponentProperties);
        }

        // Recurse into child groups and migrate any referenced child snapshots
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup child : group.getProcessGroups()) {
                migrateProcessGroupPropertiesRecursively(child, container);
                if (child.getVersionedFlowCoordinates() != null) {
                    final RegisteredFlowSnapshot childSnapshot = container.getChildSnapshot(child.getIdentifier());
                    if (childSnapshot != null && childSnapshot.getFlowContents() != null) {
                        migrateProcessGroupPropertiesRecursively(childSnapshot.getFlowContents(), container);
                    }
                }
            }
        }
    }

    private void migrateComponentProperties(final VersionedConfigurableExtension component) {
        if (component == null || component.getProperties() == null || component.getType() == null) {
            return;
        }

        try {
            final BundleCoordinate compatibleBundle = serviceFacade.getCompatibleBundle(component.getType(), FlowRegistryUtils.createBundleDto(component.getBundle()));
            final ConfigurableComponent tempComponent = serviceFacade.getTempComponent(component.getType(), compatibleBundle);
            if (tempComponent == null) {
                return;
            }

            // Prepare property configuration: raw and effective values identical; identity resolver.
            final Map<String, String> raw = new LinkedHashMap<>(component.getProperties());
            final Map<String, String> effective = new LinkedHashMap<>(raw);
            final StandardPropertyConfiguration propConfig =
                    new StandardPropertyConfiguration(
                            effective,
                            raw,
                            v -> v,
                            component.getType(),
                            new NoOpControllerServiceFactory());

            // Invoke component-level property migration if supported
            if (tempComponent instanceof Processor) {
                ((Processor) tempComponent).migrateProperties(propConfig);
            } else if (tempComponent instanceof ControllerService) {
                ((ControllerService) tempComponent).migrateProperties(propConfig);
            }

            if (propConfig.isModified()) {
                // Only persist key renames where the value did not change. Discard value additions/changes.
                final Map<String, String> originalRaw = new LinkedHashMap<>(raw);
                final Map<String, String> migratedRaw = new LinkedHashMap<>(propConfig.getRawProperties());

                // Determine removed and added keys
                final Set<String> removedKeys = new LinkedHashSet<>(originalRaw.keySet());
                removedKeys.removeAll(migratedRaw.keySet());

                final Set<String> addedKeys = new LinkedHashSet<>(migratedRaw.keySet());
                addedKeys.removeAll(originalRaw.keySet());

                // Build rename pairs where value remained exactly the same
                final Map<String, String> renames = new LinkedHashMap<>(); // oldKey -> newKey
                for (final String addedKey : addedKeys) {
                    final String addedVal = migratedRaw.get(addedKey);

                    String matchedRemoved = null;
                    for (final String removedKey : removedKeys) {
                        final String removedVal = originalRaw.get(removedKey);
                        if (Objects.equals(removedVal, addedVal)) {
                            if (matchedRemoved != null) {
                                // Ambiguous match; skip persisting this added key
                                matchedRemoved = null;
                                break;
                            }
                            matchedRemoved = removedKey;
                        }
                    }

                    if (matchedRemoved != null) {
                        renames.put(matchedRemoved, addedKey);
                    }
                }

                // Apply renames to the snapshot properties
                if (!renames.isEmpty()) {
                    final Map<String, String> snapshotProps = component.getProperties();
                    for (final Map.Entry<String, String> rename : renames.entrySet()) {
                        final String oldKey = rename.getKey();
                        final String newKey = rename.getValue();
                        final String value = snapshotProps.remove(oldKey);
                        // Only put if the key truly changed or is absent
                        if (value != null || !snapshotProps.containsKey(newKey)) {
                            snapshotProps.put(newKey, value);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            logger.debug("Preflight migration skipped for component type {} due to: {}", component.getType(), e.toString());
        }
    }

    private static final class NoOpControllerServiceFactory implements ControllerServiceFactory {
        @Override
        public ControllerServiceCreationDetails getCreationDetails(final String implementationClassName,
                                                                   final Map<String, String> serviceProperties) {
            // Strict no-op: indicate no creation and no service identifier so createControllerService returns null
            return new ControllerServiceCreationDetails(
                    null, // serviceIdentifier
                    null, // type
                    null, // bundle
                    java.util.Collections.emptyMap(),
                    null  // creationState left null to avoid creation and modification
            );
        }

        @Override
        public ControllerServiceNode create(final ControllerServiceCreationDetails creationDetails) {
            return null;
        }
    }
}
