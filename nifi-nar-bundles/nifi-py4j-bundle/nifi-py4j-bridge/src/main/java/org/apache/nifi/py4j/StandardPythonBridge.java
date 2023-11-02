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

import org.apache.nifi.python.BoundObjectCounts;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class StandardPythonBridge implements PythonBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonBridge.class);

    private volatile boolean running = false;

    private PythonProcessConfig processConfig;
    private ControllerServiceTypeLookup serviceTypeLookup;
    private PythonProcess controllerProcess;
    private final Map<ExtensionId, Integer> processorCountByType = new ConcurrentHashMap<>();
    private final Map<ExtensionId, List<PythonProcess>> processesByProcessorType = new ConcurrentHashMap<>();

    @Override
    public void initialize(final PythonBridgeInitializationContext context) {
        this.processConfig = context.getPythonProcessConfig();
        this.serviceTypeLookup = context.getControllerServiceTypeLookup();
    }

    @Override
    public synchronized void start() throws IOException {
        if (running) {
            logger.debug("{} already started, will not start again", this);
            return;
        }

        logger.debug("{} launching Python Process", this);

        try {
            final File envHome = new File(processConfig.getPythonWorkingDirectory(), "controller");
            controllerProcess = new PythonProcess(processConfig, serviceTypeLookup, envHome, "Controller", "Controller");
            controllerProcess.start();
            running = true;
        } catch (final Exception e) {
            shutdown();
            throw e;
        }
    }

    @Override
    public void discoverExtensions() {
        ensureStarted();
        final List<String> extensionsDirs = processConfig.getPythonExtensionsDirectories().stream()
            .map(File::getAbsolutePath)
            .collect(Collectors.toList());
        final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();
        controllerProcess.getController().discoverExtensions(extensionsDirs, workDirPath);
    }

    @Override
    public PythonProcessorBridge createProcessor(final String identifier, final String type, final String version, final boolean preferIsolatedProcess) {
        ensureStarted();

        final ExtensionId extensionId = getExtensionId(type, version);
        logger.debug("Creating Python Processor Type [{}] Version [{}]", extensionId.type(), extensionId.version());

        final PythonProcess pythonProcess = getProcessForNextComponent(extensionId, identifier, preferIsolatedProcess);
        final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();

        final PythonController controller = pythonProcess.getController();

        final ProcessorCreationWorkflow creationWorkflow = new ProcessorCreationWorkflow() {
            @Override
            public void downloadDependencies() {
                controller.downloadDependencies(type, version, workDirPath);
            }

            @Override
            public PythonProcessorAdapter createProcessor() {
                return controller.createProcessor(type, version, workDirPath);
            }
        };

        final PythonProcessorDetails processorDetails = controller.getProcessorDetails(type, version);
        final PythonProcessorBridge processorBridge = new StandardPythonProcessorBridge.Builder()
            .controller(controller)
            .creationWorkflow(creationWorkflow)
            .processorDetails(processorDetails)
            .workingDirectory(processConfig.getPythonWorkingDirectory())
            .moduleFile(new File(controller.getModuleFile(type, version)))
            .build();

        pythonProcess.addProcessor(identifier, preferIsolatedProcess);
        processorCountByType.merge(extensionId, 1, Integer::sum);
        return processorBridge;
    }

    @Override
    public synchronized void onProcessorRemoved(final String identifier, final String type, final String version) {
        final ExtensionId extensionId = getExtensionId(type, version);
        final List<PythonProcess> processes = processesByProcessorType.get(extensionId);
        if (processes == null) {
            return;
        }

        // Find the Python Process that has the Processor, if any, and remove it.
        // If there are no additional Processors in the Python Process, remove it from our list and shut down the process.
        final Iterator<PythonProcess> processItr = processes.iterator(); // Use iterator so we can call remove()
        while (processItr.hasNext()) {
            final PythonProcess process = processItr.next();
            final boolean removed = process.removeProcessor(identifier);
            if (removed && process.getProcessorCount() == 0) {
                processItr.remove();
                process.shutdown();
                break;
            }
        }

        processorCountByType.merge(extensionId, -1, Integer::sum);
    }

    public int getTotalProcessCount() {
        int count = 0;
        for (final List<PythonProcess> processes : processesByProcessorType.values()) {
            count += processes.size();
        }
        return count;
    }

    private synchronized PythonProcess getProcessForNextComponent(final ExtensionId extensionId, final String componentId, final boolean preferIsolatedProcess) {
        final int processorsOfThisType = processorCountByType.getOrDefault(extensionId, 0);
        final int processIndex = processorsOfThisType % processConfig.getMaxPythonProcessesPerType();

        // Check if we have any existing process that we can add the processor to.
        // We can add the processor to an existing process if either the processor to be created doesn't prefer
        // isolation (which is the case when Extension Manager creates a temp component), or if an existing process
        // consists only of processors that don't prefer isolation. I.e., we don't want to collocate two Processors if
        // they both prefer isolation.
        final List<PythonProcess> processesForType = processesByProcessorType.computeIfAbsent(extensionId, key -> new ArrayList<>());
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

                final File extensionsWorkDir = new File(processConfig.getPythonWorkingDirectory(), "extensions");
                final File componentTypeHome = new File(extensionsWorkDir, extensionId.type());
                final File envHome = new File(componentTypeHome, extensionId.version());
                final PythonProcess pythonProcess = new PythonProcess(processConfig, serviceTypeLookup, envHome, extensionId.type(), componentId);
                pythonProcess.start();

                final List<String> extensionsDirs = processConfig.getPythonExtensionsDirectories().stream()
                    .map(File::getAbsolutePath)
                    .collect(Collectors.toList());
                final String workDirPath = processConfig.getPythonWorkingDirectory().getAbsolutePath();
                pythonProcess.getController().discoverExtensions(extensionsDirs, workDirPath);

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
        return controllerProcess.getController().getProcessorTypes();
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
        controllerProcess.getController().ping();
    }

    @Override
    public String toString() {
        return "StandardPythonBridge";
    }

    private ExtensionId getExtensionId(final String type, final String version) {
        final List<PythonProcessorDetails> processorTypes = controllerProcess.getController().getProcessorTypes();
        final Optional<PythonProcessorDetails> processorTypeFound = processorTypes.stream()
                .filter(details -> details.getProcessorType().equals(type))
                .filter(details -> details.getProcessorVersion().equals(version))
                .findFirst();

        return processorTypeFound.map(details -> new ExtensionId(details.getProcessorType(), details.getProcessorVersion()))
                .orElseThrow(() -> new IllegalArgumentException(String.format("Processor Type [%s] Version [%s] not found", type, version)));
    }

    private record ExtensionId(String type, String version) {

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ExtensionId that = (ExtensionId) o;
            return Objects.equals(type, that.type) && Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, version);
        }
    }
}
