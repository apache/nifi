/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.reporting.ReportingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Task for installing a NAR that was submitted to the {@link NarManager}.
 */
public class NarInstallTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarInstallTask.class);

    private static final Set<Class<?>> ALLOWED_EXTENSION_TYPES = Set.of(
            Processor.class,
            ControllerService.class,
            ReportingTask.class,
            FlowRegistryClient.class,
            FlowAnalysisRule.class,
            ParameterProvider.class
    );

    private final NarNode narNode;
    private final NarLoader narLoader;
    private final NarManager narManager;
    private final NarComponentManager narComponentManager;
    private final ExtensionManager extensionManager;
    private final ControllerServiceProvider controllerServiceProvider;

    private NarInstallTask(final Builder builder) {
        this.narNode = Objects.requireNonNull(builder.narNode);
        this.narLoader = Objects.requireNonNull(builder.narLoader);
        this.narManager = Objects.requireNonNull(builder.narManager);
        this.narComponentManager = Objects.requireNonNull(builder.narComponentManager);
        this.extensionManager = Objects.requireNonNull(builder.extensionManager);
        this.controllerServiceProvider = Objects.requireNonNull(builder.controllerServiceProvider);
    }

    @Override
    public void run() {
        final File narFile = narNode.getNarFile();
        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        narNode.setState(NarState.INSTALLING);

        try {
            // If replacing an existing NAR with the same coordinate, then unload the existing NAR and stop+ghost any components from it
            // If the NAR being replaced contains Python extensions, those need to be included through a separate lookup since their bundle coordinate is a logical python coordinate
            final List<File> narsToLoad = new ArrayList<>();
            final StandardStoppedComponents stoppedComponents = new StandardStoppedComponents(controllerServiceProvider);
            final Bundle existingBundle = extensionManager.getBundle(coordinate);
            if (existingBundle == null) {
                LOGGER.info("Installing NAR [{}] with coordinate [{}]", narNode.getIdentifier(), coordinate);
                final List<File> unloadedNarFiles = unloadNarChain(coordinate, stoppedComponents, false);
                narsToLoad.addAll(unloadedNarFiles);
                narsToLoad.add(narFile);
            } else {
                LOGGER.info("Replacing NAR [{}], unloading existing NAR and components", coordinate);
                final List<File> unloadedNarFiles = unloadNarChain(coordinate, stoppedComponents, true);
                narsToLoad.addAll(unloadedNarFiles);
            }

            // Attempt to load the NAR and any NARs that were unloaded, or need to be reloaded, and the NAR loader will also include any NARs that were previously skipped
            // Unloading was done in child first order, so we need to reverse the list to load in parent first order
            Collections.reverse(narsToLoad);
            LOGGER.info("Install task for [{}] will load NARs {}", coordinate, narsToLoad);
            final NarLoadResult narLoadResult = narLoader.load(narsToLoad, ALLOWED_EXTENSION_TYPES);

            // For any successfully loaded NARs, un-ghost any components that can be provided by one of the loaded NARs, this handles a general ghosting case where
            // the NAR now becomes available, as well as restoring any component that may have been purposely unloaded above for replacing an existing NAR
            for (final Bundle loadedBundle : narLoadResult.getLoadedBundles()) {
                boolean installed = false;
                final BundleCoordinate loadedCoordinate = loadedBundle.getBundleDetails().getCoordinate();
                LOGGER.info("NAR [{}] was loaded", loadedCoordinate);
                if (loadedCoordinate.equals(coordinate)) {
                    // If the NAR that was just uploaded was successfully loaded, attempt to access the class of each extension to prove that each
                    // class can load successfully, if not then we want to set the state as FAILED and capture the error message
                    try {
                        verifyExtensionDefinitions(coordinate);
                        narNode.setState(NarState.INSTALLED);
                        installed = true;
                    } catch (final Throwable t) {
                        LOGGER.error("Failed to install NAR [{}]", coordinate, t);
                        narNode.setFailure(t);
                    }
                } else {
                    try {
                        verifyExtensionDefinitions(loadedCoordinate);
                        narManager.updateState(loadedCoordinate, NarState.INSTALLED);
                        installed = true;
                    } catch (final NarNotFoundException e) {
                        LOGGER.warn("NAR [{}] was loaded, but no longer exists in the NAR Manager", loadedCoordinate);
                    } catch (final Throwable t) {
                        LOGGER.error("Failed to install NAR [{}]", coordinate, t);
                        narManager.updateFailed(loadedCoordinate, t);
                    }
                }

                if (installed) {
                    try {
                        final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>(extensionManager.getTypes(loadedCoordinate));
                        extensionDefinitions.addAll(extensionManager.getPythonExtensions(loadedCoordinate));
                        narComponentManager.loadMissingComponents(loadedCoordinate, extensionDefinitions, stoppedComponents);
                    } catch (final Throwable t) {
                        LOGGER.error("Failed to load missing components from [{}]", loadedCoordinate, t);
                    }
                }
            }

            // Process the skipped bundles and mark them as having a missing dependency
            for (final BundleDetails skippedBundles : narLoadResult.getSkippedBundles()) {
                final BundleCoordinate skippedCoordinate = skippedBundles.getCoordinate();
                LOGGER.info("NAR [{}] is missing dependency", skippedCoordinate);
                if (skippedCoordinate.equals(coordinate)) {
                    narNode.setState(NarState.MISSING_DEPENDENCY);
                } else {
                    try {
                        narManager.updateState(skippedCoordinate, NarState.MISSING_DEPENDENCY);
                    } catch (final NarNotFoundException e) {
                        LOGGER.warn("NAR [{}] was skipped, but no longer exists in the NAR Manager", skippedCoordinate);
                    }
                }
            }

            // Restore previously running/enabled components to their original state
            stoppedComponents.startAll();

            // Notify the NAR Manager that the install task completed for the current NAR
            narManager.completeInstall(narNode.getIdentifier());

        } catch (final Throwable t) {
            LOGGER.error("Failed to install NAR [{}]", coordinate, t);
            narNode.setFailure(t);
        }
    }

    private void verifyExtensionDefinitions(final BundleCoordinate coordinate) {
        final Set<ExtensionDefinition> loadedExtensionDefinitions = extensionManager.getTypes(coordinate);
        for (final ExtensionDefinition loadedExtensionDefinition : loadedExtensionDefinitions) {
            final Class<?> extensionClass = extensionManager.getClass(loadedExtensionDefinition);
            LOGGER.debug("Loaded [{}] from bundle [{}]", extensionClass.getCanonicalName(), coordinate);
        }
    }

    private List<File> unloadNarChain(final BundleCoordinate coordinate, final StoppedComponents stoppedComponents, final boolean includeStartingNar) {
        final Map<BundleCoordinate, File> narFilesByCoordinate = new HashMap<>();
        narManager.getNars().forEach(narNode -> narFilesByCoordinate.put(narNode.getManifest().getCoordinate(), narNode.getNarFile()));

        final List<File> unloadedNarFiles = new ArrayList<>();
        final List<Bundle> bundlesToUnload = new ArrayList<>();
        final Map<BundleCoordinate, Set<ExtensionDefinition>> extensionsByCoordinate = new LinkedHashMap<>();

        final List<Bundle> bundleChain = getDependencyChain(coordinate, includeStartingNar);
        for (final Bundle chainedBundle : bundleChain) {
            final BundleCoordinate chainedCoordinate = chainedBundle.getBundleDetails().getCoordinate();
            final File chainedNarFile = narFilesByCoordinate.get(chainedCoordinate);
            if (chainedNarFile != null) {
                final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>(extensionManager.getTypes(chainedCoordinate));
                extensionDefinitions.addAll(extensionManager.getPythonExtensions(chainedCoordinate));
                extensionsByCoordinate.put(chainedCoordinate, extensionDefinitions);

                bundlesToUnload.add(chainedBundle);
                unloadedNarFiles.add(chainedNarFile);
            } else {
                LOGGER.warn("Found NAR [{}] in dependency chain of [{}], but it is not present in the NAR Manager", chainedCoordinate, coordinate);
            }
        }

        LOGGER.debug("Unloading NAR chain from [{}] requires unloading {}", coordinate, bundlesToUnload);
        narLoader.unload(bundlesToUnload);

        for (Map.Entry<BundleCoordinate, Set<ExtensionDefinition>> entry : extensionsByCoordinate.entrySet()) {
            narComponentManager.unloadComponents(entry.getKey(), entry.getValue(), stoppedComponents);
        }

        return unloadedNarFiles;
    }

    /**
     * Gets the depth-first list of dependents from this bundle, including the starting bundle.
     *
     * If bundle A is dependent on B which is dependent on C, then...
     *   - getDependencyChain(A) returns [A]
     *   - getDependencyChain(B) returns [A, B]
     *   - getDependencyChain(C) returns [A, B, C]
     *
     * @param startingCoordinate the coordinate to start building the chain from
     * @param includeStartingBundle indicates if the bundle for the starting coordinate should be included in the chain
     * @return the depth-first list of dependents
     */
    private List<Bundle> getDependencyChain(final BundleCoordinate startingCoordinate, final boolean includeStartingBundle) {
        final List<Bundle> dependencyChain = new ArrayList<>();
        traverseDependencyChain(startingCoordinate, dependencyChain);
        if (includeStartingBundle) {
            final Bundle startingBundle = extensionManager.getBundle(startingCoordinate);
            if (startingBundle != null) {
                dependencyChain.add(startingBundle);
            }
        }
        return dependencyChain;
    }

    private void traverseDependencyChain(final BundleCoordinate currentCoordinate, final List<Bundle> dependencyChain) {
        final Set<Bundle> dependentBundles = extensionManager.getDependentBundles(currentCoordinate);
        if (dependentBundles.isEmpty()) {
            return;
        }
        for (final Bundle dependentBundle : dependentBundles) {
            final BundleCoordinate dependentCoordinate = dependentBundle.getBundleDetails().getCoordinate();
            traverseDependencyChain(dependentCoordinate, dependencyChain);
            dependencyChain.add(dependentBundle);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private NarNode narNode;
        private NarLoader narLoader;
        private NarManager narManager;
        private NarComponentManager narComponentManager;
        private ExtensionManager extensionManager;
        private ControllerServiceProvider controllerServiceProvider;

        public Builder narNode(final NarNode narNode) {
            this.narNode = narNode;
            return this;
        }

        public Builder narLoader(final NarLoader narLoader) {
            this.narLoader = narLoader;
            return this;
        }

        public Builder narManager(final NarManager narManager) {
            this.narManager = narManager;
            return this;
        }

        public Builder narComponentManager(final NarComponentManager narComponentManager) {
            this.narComponentManager = narComponentManager;
            return this;
        }

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder controllerServiceProvider(final ControllerServiceProvider controllerServiceProvider) {
            this.controllerServiceProvider = controllerServiceProvider;
            return this;
        }

        public NarInstallTask build() {
            return new NarInstallTask(this);
        }
    }
}
