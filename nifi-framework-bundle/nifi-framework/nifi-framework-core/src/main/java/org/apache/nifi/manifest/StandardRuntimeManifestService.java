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
package org.apache.nifi.manifest;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.extension.manifest.AllowableValue;
import org.apache.nifi.extension.manifest.ControllerServiceDefinition;
import org.apache.nifi.extension.manifest.Dependency;
import org.apache.nifi.extension.manifest.DependentValues;
import org.apache.nifi.extension.manifest.ExpressionLanguageScope;
import org.apache.nifi.extension.manifest.Extension;
import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.InputRequirement;
import org.apache.nifi.extension.manifest.MultiProcessorUseCase;
import org.apache.nifi.extension.manifest.ProcessorConfiguration;
import org.apache.nifi.extension.manifest.Property;
import org.apache.nifi.extension.manifest.UseCase;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.PythonBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails;
import org.apache.nifi.python.processor.documentation.ProcessorConfigurationDetails;
import org.apache.nifi.python.processor.documentation.PropertyDescription;
import org.apache.nifi.python.processor.documentation.UseCaseDetails;
import org.apache.nifi.runtime.manifest.ExtensionManifestContainer;
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;
import org.apache.nifi.runtime.manifest.impl.SchedulingDefaultsFactory;
import org.apache.nifi.runtime.manifest.impl.StandardRuntimeManifestBuilder;
import org.apache.nifi.web.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardRuntimeManifestService implements RuntimeManifestService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardRuntimeManifestService.class);

    private static final String RUNTIME_MANIFEST_IDENTIFIER = "nifi";
    private static final String RUNTIME_TYPE = "nifi";

    private final ExtensionManager extensionManager;
    private final ExtensionManifestParser extensionManifestParser;
    private final String runtimeManifestIdentifier;
    private final String runtimeType;

    public StandardRuntimeManifestService(final ExtensionManager extensionManager, final ExtensionManifestParser extensionManifestParser,
                                          final String runtimeManifestIdentifier, final String runtimeType) {
        this.extensionManager = extensionManager;
        this.extensionManifestParser = extensionManifestParser;
        this.runtimeManifestIdentifier = runtimeManifestIdentifier;
        this.runtimeType = runtimeType;
    }

    public StandardRuntimeManifestService(final ExtensionManager extensionManager, final ExtensionManifestParser extensionManifestParser) {
        this(extensionManager, extensionManifestParser, RUNTIME_MANIFEST_IDENTIFIER, RUNTIME_TYPE);
    }

    @Override
    public RuntimeManifest getManifest() {
        final Set<Bundle> allBundles = extensionManager.getAllBundles();
        final RuntimeManifestBuilder manifestBuilder = createRuntimeManifestBuilder();

        for (final Bundle bundle : allBundles) {
            getExtensionManifest(bundle).ifPresent(manifestBuilder::addBundle);
        }

        getPythonExtensionManifests().forEach(manifestBuilder::addBundle);

        return manifestBuilder.build();
    }

    @Override
    public RuntimeManifest getManifestForBundle(String group, String artifact, String version) {
        final Set<Bundle> allBundles = extensionManager.getAllBundles();
        final RuntimeManifestBuilder manifestBuilder = createRuntimeManifestBuilder();

        if (PythonBundle.isPythonCoordinate(group, artifact)) {
            getPythonExtensionManifests().forEach(manifestBuilder::addBundle);
        } else {
            final Optional<Bundle> desiredBundle = allBundles.stream()
                    .filter(bundle -> group.equals(bundle.getBundleDetails().getCoordinate().getGroup())
                            && artifact.equals(bundle.getBundleDetails().getCoordinate().getId())
                            && version.equals(bundle.getBundleDetails().getCoordinate().getVersion())).findFirst();

            desiredBundle.ifPresent(bundle -> getExtensionManifest(bundle).ifPresent(manifestBuilder::addBundle));
        }

        return manifestBuilder.build();
    }

    private RuntimeManifestBuilder createRuntimeManifestBuilder() {
        final Bundle frameworkBundle = getFrameworkBundle();
        final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
        final Date frameworkBuildDate = frameworkDetails.getBuildTimestampDate();

        final BuildInfo buildInfo = new BuildInfo();
        buildInfo.setVersion(frameworkDetails.getCoordinate().getVersion());
        buildInfo.setRevision(frameworkDetails.getBuildRevision());
        buildInfo.setCompiler(frameworkDetails.getBuildJdk());
        buildInfo.setTimestamp(frameworkBuildDate == null ? null : frameworkBuildDate.getTime());

        final RuntimeManifestBuilder manifestBuilder = new StandardRuntimeManifestBuilder()
                .identifier(runtimeManifestIdentifier)
                .runtimeType(runtimeType)
                .version(buildInfo.getVersion())
                .schedulingDefaults(SchedulingDefaultsFactory.getNifiSchedulingDefaults())
                .buildInfo(buildInfo);

        return manifestBuilder;
    }

    private List<ExtensionManifestContainer> getPythonExtensionManifests() {
        final Map<String, List<Extension>> extensionsPerVersion = new HashMap<>();
        final Set<ExtensionDefinition> processorDefinitions = extensionManager.getExtensions(Processor.class);
        for (final ExtensionDefinition definition : processorDefinitions) {
            if (!PythonBundle.isPythonCoordinate(definition.getBundle().getBundleDetails().getCoordinate())) {
                continue;
            }

            final PythonProcessorDetails pythonProcessorDetails = extensionManager.getPythonProcessorDetails(definition.getImplementationClassName(), definition.getVersion());
            if (pythonProcessorDetails == null) {
                LOGGER.debug("Could not find Python Processor Details for {} version {}", definition.getImplementationClassName(), definition.getVersion());
                continue;
            }

            final Extension extension = createExtension(pythonProcessorDetails);
            final List<Extension> extensions = extensionsPerVersion.computeIfAbsent(definition.getVersion(), version -> new ArrayList<>());
            extensions.add(extension);
        }

        if (extensionsPerVersion.isEmpty()) {
            return List.of();
        }

        final List<ExtensionManifestContainer> containers = new ArrayList<>();
        for (final Map.Entry<String, List<Extension>> entry : extensionsPerVersion.entrySet()) {
            final String version = entry.getKey();
            final List<Extension> extensions = entry.getValue();

            final ExtensionManifest extensionManifest = new ExtensionManifest();
            extensionManifest.setGroupId(PythonBundle.GROUP_ID);
            extensionManifest.setArtifactId(PythonBundle.ARTIFACT_ID);
            extensionManifest.setVersion(version);
            extensionManifest.setExtensions(extensions);

            containers.add(new ExtensionManifestContainer(extensionManifest, Map.of()));
        }

        return containers;
    }

    private Extension createExtension(final PythonProcessorDetails pythonProcessorDetails) {
        final Extension extension = new Extension();
        extension.setDescription(pythonProcessorDetails.getCapabilityDescription());
        extension.setName(pythonProcessorDetails.getProcessorType());
        extension.setTags(pythonProcessorDetails.getTags());
        extension.setInputRequirement(InputRequirement.INPUT_REQUIRED);
        extension.setSupportsBatching(true);
        extension.setType(ExtensionType.PROCESSOR);
        extension.setTriggerWhenEmpty(false);
        extension.setTriggerSerially(false);
        extension.setTriggerWhenAnyDestinationAvailable(false);

        final List<org.apache.nifi.extension.manifest.Relationship> relationships = new ArrayList<>();
        extension.setRelationships(relationships);
        for (final String relationshipName : List.of("success", "failure", "original")) {
            final org.apache.nifi.extension.manifest.Relationship relationship = new org.apache.nifi.extension.manifest.Relationship();
            relationship.setAutoTerminated(false);
            relationship.setName(relationshipName);
            relationships.add(relationship);
        }

        final List<UseCase> useCases = getUseCases(pythonProcessorDetails);
        extension.setUseCases(useCases);

        final List<MultiProcessorUseCase> multiProcessorUseCases = getMultiProcessorUseCases(pythonProcessorDetails);
        extension.setMultiProcessorUseCases(multiProcessorUseCases);

        final List<PropertyDescription> propertyDescriptions = pythonProcessorDetails.getPropertyDescriptions();
        final List<Property> manifestProperties = propertyDescriptions == null ? List.of() : propertyDescriptions.stream()
            .map(StandardRuntimeManifestService::createManifestProperty)
            .toList();
        extension.setProperties(manifestProperties);

        return extension;
    }

    private static Property createManifestProperty(final PropertyDescription propertyDescription) {
        final Property property = new Property();
        property.setName(propertyDescription.getName());
        property.setDescription(propertyDescription.getDescription());
        property.setDefaultValue(propertyDescription.getDefaultValue());
        property.setDisplayName(propertyDescription.getDisplayName());
        property.setDynamicallyModifiesClasspath(false);
        property.setDynamic(false);
        try {
            property.setExpressionLanguageScope(ExpressionLanguageScope.valueOf(propertyDescription.getExpressionLanguageScope()));
        } catch (final Exception e) {
            property.setExpressionLanguageScope(ExpressionLanguageScope.NONE);
        }

        property.setRequired(propertyDescription.isRequired());
        property.setSensitive(propertyDescription.isSensitive());

        property.setControllerServiceDefinition(getManifestControllerServiceDefinition(propertyDescription.getControllerServiceDefinition()));
        property.setAllowableValues(getAllowableValues(propertyDescription));

        property.setDependencies(getDependencies(propertyDescription));

        return property;
    }

    private static List<Dependency> getDependencies(final PropertyDescription propertyDescription) {
        return Optional.ofNullable(propertyDescription.getDependencies()).orElse(List.of())
                .stream()
                .map(value -> {
                    DependentValues dependentValues = new DependentValues();
                    dependentValues.setValues(value.getDependentValues());

                    Dependency dependency = new Dependency();
                    dependency.setPropertyName(value.getName());
                    dependency.setPropertyDisplayName(value.getDisplayName());
                    dependency.setDependentValues(dependentValues);

                    return dependency;
                })
                .toList();
    }

    private static ControllerServiceDefinition getManifestControllerServiceDefinition(final String controllerServiceClassName) {
        if (controllerServiceClassName == null) {
            return null;
        }

        final ControllerServiceDefinition definition = new ControllerServiceDefinition();
        definition.setClassName(controllerServiceClassName);
        return definition;
    }

    private static List<AllowableValue> getAllowableValues(final PropertyDescription propertyDescription) {
        return Optional.ofNullable(propertyDescription.getAllowableValues()).orElse(List.of())
            .stream()
            .map(value -> {
                AllowableValue allowableValue = new AllowableValue();
                allowableValue.setValue(value);
                allowableValue.setDisplayName(value);
                return allowableValue;
            })
            .toList();
    }

    private static List<UseCase> getUseCases(final PythonProcessorDetails pythonProcessorDetails) {
        final List<UseCase> useCases = new ArrayList<>();
        for (final UseCaseDetails useCaseDetails : pythonProcessorDetails.getUseCases()) {
            final UseCase useCase = new UseCase();
            useCases.add(useCase);

            useCase.setDescription(useCaseDetails.getDescription());
            useCase.setNotes(useCaseDetails.getNotes());
            useCase.setKeywords(useCaseDetails.getKeywords());
            useCase.setInputRequirement(InputRequirement.INPUT_REQUIRED);
            useCase.setConfiguration(useCaseDetails.getConfiguration());
        }
        return useCases;
    }

    private static List<MultiProcessorUseCase> getMultiProcessorUseCases(final PythonProcessorDetails pythonProcessorDetails) {
        final List<MultiProcessorUseCase> multiProcessorUseCases = new ArrayList<>();
        for (final MultiProcessorUseCaseDetails useCaseDetails : pythonProcessorDetails.getMultiProcessorUseCases()) {
            final MultiProcessorUseCase useCase = new MultiProcessorUseCase();
            multiProcessorUseCases.add(useCase);

            useCase.setDescription(useCaseDetails.getDescription());
            useCase.setNotes(useCaseDetails.getNotes());
            useCase.setKeywords(useCaseDetails.getKeywords());

            final List<ProcessorConfiguration> processorConfigurations = new ArrayList<>();
            useCase.setProcessorConfigurations(processorConfigurations);
            for (final ProcessorConfigurationDetails processorConfig : useCaseDetails.getConfigurations()) {
                final ProcessorConfiguration processorConfiguration = new ProcessorConfiguration();
                processorConfigurations.add(processorConfiguration);

                processorConfiguration.setConfiguration(processorConfig.getConfiguration());
                processorConfiguration.setProcessorClassName(processorConfig.getProcessorType());
            }
        }

        return multiProcessorUseCases;
    }

    private Optional<ExtensionManifestContainer> getExtensionManifest(final Bundle bundle) {
        final BundleDetails bundleDetails = bundle.getBundleDetails();
        try {
            final Optional<ExtensionManifest> extensionManifest = loadExtensionManifest(bundleDetails);
            if (extensionManifest.isEmpty()) {
                return Optional.empty();
            }

            final Map<String, String> additionalDetails = loadAdditionalDetails(bundleDetails);

            final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest.get(), additionalDetails);
            return Optional.of(container);
        } catch (final IOException e) {
            LOGGER.error("Unable to load extension manifest for bundle [{}]", bundleDetails.getCoordinate(), e);
            return Optional.empty();
        }
    }

    private Optional<ExtensionManifest> loadExtensionManifest(final BundleDetails bundleDetails) throws IOException {
        final File manifestFile = new File(bundleDetails.getWorkingDirectory(), "META-INF/docs/extension-manifest.xml");
        if (!manifestFile.exists()) {
            LOGGER.warn("There is no extension manifest for bundle [{}]", bundleDetails.getCoordinate());
            return Optional.empty();
        }

        try (final InputStream inputStream = new FileInputStream(manifestFile)) {
            final ExtensionManifest extensionManifest = extensionManifestParser.parse(inputStream);
            // Newer NARs will have these fields populated in extension-manifest.xml, but older NARs will not, so we can
            // set the values from the BundleCoordinate which already has the group, artifact id, and version
            extensionManifest.setGroupId(bundleDetails.getCoordinate().getGroup());
            extensionManifest.setArtifactId(bundleDetails.getCoordinate().getId());
            extensionManifest.setVersion(bundleDetails.getCoordinate().getVersion());
            return Optional.of(extensionManifest);
        }
    }

    @Override
    public Map<String, File> discoverAdditionalDetails(String group, String artifact, String version) {
        final BundleCoordinate bundleCoordinate = new BundleCoordinate(group, artifact, version);
        final Bundle bundle = extensionManager.getBundle(bundleCoordinate);

        if (bundle == null) {
            throw new ResourceNotFoundException("Unable to find bundle [" + bundleCoordinate + "]");
        }

        return discoverAdditionalDetails(bundle.getBundleDetails());
    }

    private Map<String, File> discoverAdditionalDetails(final BundleDetails bundleDetails) {
        final Map<String, File> additionalDetailsMap = new LinkedHashMap<>();

        final File additionalDetailsDir = new File(bundleDetails.getWorkingDirectory(), "META-INF/docs/additional-details");
        if (!additionalDetailsDir.exists()) {
            LOGGER.debug("No additional-details directory found under [{}]", bundleDetails.getWorkingDirectory().getAbsolutePath());
            return additionalDetailsMap;
        }

        for (final File additionalDetailsTypeDir : additionalDetailsDir.listFiles()) {
            if (!additionalDetailsTypeDir.isDirectory()) {
                LOGGER.debug("Skipping [{}], not a directory...", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            final File additionalDetailsFile = new File(additionalDetailsTypeDir, "additionalDetails.md");
            if (!additionalDetailsFile.exists()) {
                LOGGER.debug("No additionalDetails.md found under [{}]", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            additionalDetailsMap.put(additionalDetailsTypeDir.getName(), additionalDetailsFile);
        }

        return additionalDetailsMap;
    }

    private Map<String, String> loadAdditionalDetails(final BundleDetails bundleDetails) {
        final Map<String, File> additionalDetailsMap = discoverAdditionalDetails(bundleDetails);

        return additionalDetailsMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            final File additionalDetailsFile = entry.getValue();

            try (final Stream<String> additionalDetailsLines = Files.lines(additionalDetailsFile.toPath())) {
                final String typeName = additionalDetailsFile.getParentFile().getName();
                final String additionalDetailsContent = additionalDetailsLines.collect(Collectors.joining());
                LOGGER.debug("Added additionalDetails for {} from {}", typeName, additionalDetailsFile.getAbsolutePath());
                return additionalDetailsContent;
            } catch (final IOException e) {
                throw new RuntimeException("Unable to load additional details content for "
                        + additionalDetailsFile.getAbsolutePath() + " due to: " + e.getMessage(), e);
            }
        }));
    }

    // Visible for overriding from tests
    Bundle getFrameworkBundle() {
        return NarClassLoadersHolder.getInstance().getFrameworkBundle();
    }

}
