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

package org.apache.nifi.py4j;

import org.apache.nifi.components.AsyncLoadedProcessor;
import org.apache.nifi.py4j.logback.LevelChangeListener;
import org.apache.nifi.py4j.logging.LogLevelChangeHandler;
import org.apache.nifi.py4j.logging.StandardLogLevelChangeHandler;
import org.apache.nifi.python.BoundObjectCounts;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.FlowFileSource;
import org.apache.nifi.python.processor.FlowFileSourceProxy;
import org.apache.nifi.python.processor.FlowFileTransform;
import org.apache.nifi.python.processor.FlowFileTransformProxy;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.RecordTransform;
import org.apache.nifi.python.processor.RecordTransformProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StandardPythonBridge implements PythonBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonBridge.class);

    private volatile boolean running = false;

    private PythonProcessConfig processConfig;
    private ControllerServiceTypeLookup serviceTypeLookup;
    private Supplier<Set<File>> narDirectoryLookup;
    private PythonProcess controllerProcess;
    private final Map<ExtensionId, Integer> processorCountByType = new ConcurrentHashMap<>();
    private final Map<ExtensionId, List<PythonProcess>> processesByProcessorType = new ConcurrentHashMap<>();

    @Override
    public void initialize(final PythonBridgeInitializationContext context) {
        this.processConfig = context.getPythonProcessConfig();
        this.serviceTypeLookup = context.getControllerServiceTypeLookup();
        this.narDirectoryLookup = context.getNarDirectoryLookup();
    }

    @Override
    public synchronized void start() throws IOException {
        if (running) {
            logger.debug("{} already started, will not start again", this);
            return;
        }

        logger.debug("{} launching Python Process", this);

        try {
            final LogLevelChangeHandler logLevelChangeHandler = StandardLogLevelChangeHandler.getHandler();
            LevelChangeListener.registerLogbackListener(logLevelChangeHandler);

            final File envHome = new File(processConfig.getPythonWorkingDirectory(), "controller");
            controllerProcess = new PythonProcess(processConfig, serviceTypeLookup, envHome, true, "Controller", "Controller");
            controllerProcess.start();
            running = true;
        } catch (final Exception e) {
            shutdown();
            throw e;
        }
    }

    @Override
    public void discoverExtensions(final boolean includeNarDirectories) {
        ensureStarted();
        final List<String> extensionsDirs = processConfig.getPythonExtensionsDirectories().stream()
            .map(File::getAbsolutePath)
            .collect(Collectors.toCollection(ArrayList::new));

        if (includeNarDirectories) {
            extensionsDirs.addAll(getNarDirectories());
        }

        final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();
        controllerProcess.discoverExtensions(extensionsDirs, workDirPath);
    }

    @Override
    public void discoverExtensions(final List<File> extensionDirectories) {
        ensureStarted();
        final List<String> extensionsDirs = extensionDirectories.stream()
                .map(File::getAbsolutePath)
                .toList();

        final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();
        controllerProcess.discoverExtensions(extensionsDirs, workDirPath);
    }

    private PythonProcessorBridge createProcessorBridge(final String identifier, final String type, final String version, final boolean preferIsolatedProcess) {
        ensureStarted();

        final Optional<ExtensionId> extensionIdFound = findExtensionId(type, version);
        final ExtensionId extensionId = extensionIdFound.orElseThrow(() -> new IllegalArgumentException("Processor Type [%s] Version [%s] not found".formatted(type, version)));
        logger.debug("Creating Python Processor Type [{}] Version [{}]", extensionId.type(), extensionId.version());

        final PythonProcessorDetails processorDetails = getProcessorTypes().stream()
            .filter(details -> details.getProcessorType().equals(type))
            .filter(details -> details.getProcessorVersion().equals(version))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Could not find Processor Details for Python Processor type [%s] or version [%s]".formatted(type, version)));

        final String processorHome = processorDetails.getExtensionHome();
        final boolean bundledWithDependencies = processorDetails.isBundledWithDependencies();

        final PythonProcess pythonProcess = getProcessForNextComponent(extensionId, identifier, processorHome, preferIsolatedProcess, bundledWithDependencies);
        final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();

        final PythonProcessorBridge processorBridge = pythonProcess.createProcessor(identifier, type, version, workDirPath, preferIsolatedProcess);
        processorCountByType.merge(extensionId, 1, Integer::sum);
        return processorBridge;
    }

    @Override
    public AsyncLoadedProcessor createProcessor(final String identifier, final String type, final String version, final boolean preferIsolatedProcess, final boolean initialize) {
        final PythonProcessorDetails processorDetails = getProcessorTypes().stream()
            .filter(details -> details.getProcessorType().equals(type))
            .filter(details -> details.getProcessorVersion().equals(version))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown Python Processor type [%s] or version [%s]".formatted(type, version)));

        final String implementedInterface = processorDetails.getInterface();
        final Supplier<PythonProcessorBridge> processorBridgeFactory = () -> createProcessorBridge(identifier, type, version, preferIsolatedProcess);

        if (FlowFileTransform.class.getName().equals(implementedInterface)) {
            return new FlowFileTransformProxy(type, processorBridgeFactory, initialize);
        }
        if (RecordTransform.class.getName().equals(implementedInterface)) {
            return new RecordTransformProxy(type, processorBridgeFactory, initialize);
        }
        if (FlowFileSource.class.getName().equals(implementedInterface)) {
            return new FlowFileSourceProxy(type, processorBridgeFactory, initialize);
        }
        return null;
    }

    @Override
    public synchronized void onProcessorRemoved(final String identifier, final String type, final String version) {
        final Optional<ExtensionId> extensionIdFound = findExtensionId(type, version);

        if (extensionIdFound.isPresent()) {
            final ExtensionId extensionId = extensionIdFound.get();
            final List<PythonProcess> processes = processesByProcessorType.get(extensionId);
            if (processes == null) {
                return;
            }

            Thread.ofVirtual().name("Remove Python Processor " + identifier).start(() -> {
                PythonProcess toRemove = null;

                try {
                    // Find the Python Process that has the Processor, if any, and remove it.
                    // If there are no additional Processors in the Python Process, remove it from our list and shut down the process.
                    // Use iterator so we can call remove()
                    for (final PythonProcess process : processes) {
                        final boolean removed = process.removeProcessor(identifier);
                        if (removed && process.getProcessorCount() == 0) {
                            toRemove = process;
                            break;
                        }
                    }

                    if (toRemove != null) {
                        processes.remove(toRemove);
                        toRemove.shutdown();
                    }
                } catch (final Exception e) {
                    logger.error("Failed to trigger removal of Python Processor with ID {}", identifier, e);
                }
            });

            processorCountByType.merge(extensionId, -1, Integer::sum);
        } else {
            logger.debug("Processor Type [{}] Version [{}] not found", type, version);
        }
    }

    public int getTotalProcessCount() {
        int count = 0;
        for (final List<PythonProcess> processes : processesByProcessorType.values()) {
            count += processes.size();
        }
        return count;
    }

    private synchronized PythonProcess getProcessForNextComponent(final ExtensionId extensionId, final String componentId, final String processorHome, final boolean preferIsolatedProcess,
                final boolean packagedWithDependencies) {

        final int processorsOfThisType = processorCountByType.getOrDefault(extensionId, 0);
        final int processIndex = processorsOfThisType % processConfig.getMaxPythonProcessesPerType();

        // Check if we have any existing process that we can add the processor to.
        // We can add the processor to an existing process if either the processor to be created doesn't prefer
        // isolation (which is the case when Extension Manager creates a temp component), or if an existing process
        // consists only of processors that don't prefer isolation. I.e., we don't want to collocate two Processors if
        // they both prefer isolation.
        final List<PythonProcess> processesForType = processesByProcessorType.computeIfAbsent(extensionId, key -> new CopyOnWriteArrayList<>());
        for (final PythonProcess pythonProcess : processesForType) {
            if (!preferIsolatedProcess || !pythonProcess.containsIsolatedProcessor()) {
                logger.debug("Using {} to create Processor of type {}", pythonProcess, extensionId.type());
                return pythonProcess;
            }
        }

        if (processesForType.size() <= processIndex) {
            try {
                // Make sure that we don't have too many processes already launched.
                final int totalProcessCount = getTotalProcessCount();
                if (totalProcessCount >= processConfig.getMaxPythonProcesses()) {
                    throw new IllegalStateException("Cannot launch new Python Process because the maximum number of processes allowed, according to nifi.properties, is " +
                        processConfig.getMaxPythonProcesses() + " and " + "there are currently " + totalProcessCount + " processes active");
                }

                logger.info("In order to create Python Processor of type {}, launching a new Python Process because there are currently {} Python Processors of this type and {} Python Processes",
                    extensionId.type(), processorsOfThisType, processesByProcessorType.size());

                // If the processor is packaged with its dependencies as a NAR, we can use the Processor Home as the Environment Home.
                // Otherwise, we need to create a Virtual Environment for the Processor.
                final File envHome;
                if (packagedWithDependencies) {
                    envHome = new File(processorHome);
                } else {
                    final File extensionsWorkDir = new File(processConfig.getPythonWorkingDirectory(), "extensions");
                    final File componentTypeHome = new File(extensionsWorkDir, extensionId.type());
                    envHome = new File(componentTypeHome, extensionId.version());
                }

                final PythonProcess pythonProcess = new PythonProcess(processConfig, serviceTypeLookup, envHome, packagedWithDependencies, extensionId.type(), componentId);
                pythonProcess.start();

                // Create list of extensions directories, including NAR directories
                final List<String> extensionsDirs = processConfig.getPythonExtensionsDirectories().stream()
                    .map(File::getAbsolutePath)
                    .collect(Collectors.toCollection(ArrayList::new));
                extensionsDirs.addAll(getNarDirectories());

                final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();
                pythonProcess.discoverExtensions(extensionsDirs, workDirPath);

                // Add the newly create process to the processes for the given type of processor.
                processesForType.add(pythonProcess);

                return pythonProcess;
            } catch (final IOException ioe) {
                final String message = String.format("Failed to launch Process for Python Processor [%s] Version [%s]", extensionId.type(), extensionId.version());
                throw new RuntimeException(message, ioe);
            }
        } else {
            final PythonProcess pythonProcess = processesForType.get(processIndex);
            logger.warn("Using existing process {} to create Processor of type {} because configuration indicates that no more than {} processes " +
                "should be created for any Processor Type. This may result in slower performance for Processors of this type",
                pythonProcess, extensionId.type(), processConfig.getMaxPythonProcessesPerType());

            return pythonProcess;
        }
    }

    @Override
    public List<PythonProcessorDetails> getProcessorTypes() {
        ensureStarted();
        return controllerProcess.getCurrentController().getProcessorTypes();
    }

    @Override
    public synchronized Map<String, Integer> getProcessCountsPerType() {
        final Map<String, Integer> counts = new HashMap<>(processesByProcessorType.size());

        for (final Map.Entry<ExtensionId, List<PythonProcess>> entry : processesByProcessorType.entrySet()) {
            counts.put(entry.getKey().type() + " version " + entry.getKey().version(), entry.getValue().size());
        }

        return counts;
    }

    @Override
    public void removeProcessorType(final String type, final String version) {
        ensureStarted();
        controllerProcess.getCurrentController().removeProcessorType(type, version);
    }


    @Override
    public synchronized List<BoundObjectCounts> getBoundObjectCounts() {
        final List<BoundObjectCounts> list = new ArrayList<>();

        for (final Map.Entry<ExtensionId, List<PythonProcess>> entry : processesByProcessorType.entrySet()) {
            final ExtensionId extensionId = entry.getKey();
            final List<PythonProcess> processes = entry.getValue();

            for (final PythonProcess process : processes) {
                final Map<String, Integer> counts = process.getJavaObjectBindingCounts();
                final BoundObjectCounts boundObjectCounts = new StandardBoundObjectCounts(process.toString(), extensionId.type(), extensionId.version(), counts);
                list.add(boundObjectCounts);
            }
        }

        return list;
    }

    private void ensureStarted() {
        if (!running) {
            throw new IllegalStateException("Cannot perform action because " + this + " is not currently running");
        }
    }

    @Override
    public synchronized void shutdown() {
        logger.info("Shutting down Python Server");

        running = false;

        for (final List<PythonProcess> processes : processesByProcessorType.values()) {
            for (final PythonProcess process : processes) {
                process.shutdown();
            }
        }

        if (controllerProcess != null) {
            controllerProcess.shutdown();
        }

        logger.info("Successfully shutdown Python Server");
    }

    @Override
    public void ping() {
        controllerProcess.getCurrentController().ping();
    }

    @Override
    public String toString() {
        return "StandardPythonBridge";
    }

    private Set<String> getNarDirectories() {
        return narDirectoryLookup.get().stream()
                .map(File::getAbsolutePath)
                .collect(Collectors.toSet());
    }

    private Optional<ExtensionId> findExtensionId(final String type, final String version) {
        final List<PythonProcessorDetails> processorTypes = controllerProcess.getCurrentController().getProcessorTypes();
        return processorTypes.stream()
                .filter(details -> details.getProcessorType().equals(type))
                .filter(details -> details.getProcessorVersion().equals(version))
                .map(details -> new ExtensionId(details.getProcessorType(), details.getProcessorVersion()))
                .findFirst();
    }

    private record ExtensionId(String type, String version) {
    }
}
