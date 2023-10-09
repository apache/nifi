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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.FlowFileTransform;
import org.apache.nifi.python.processor.FlowFileTransformProxy;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.PythonProcessorInitializationContext;
import org.apache.nifi.python.processor.PythonProcessorProxy;
import org.apache.nifi.python.processor.RecordTransform;
import org.apache.nifi.python.processor.RecordTransformProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState;

public class StandardPythonProcessorBridge implements PythonProcessorBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonProcessorBridge.class);

    private final PythonController controller;
    private final ProcessorCreationWorkflow creationWorkflow;
    private final PythonProcessorDetails processorDetails;
    private volatile PythonProcessorAdapter adapter;
    private final PythonProcessorProxy proxy;
    private final File workingDir;
    private final File moduleFile;
    private volatile long lastModified;
    private volatile LoadState loadState = LoadState.DOWNLOADING_DEPENDENCIES;
    private volatile PythonProcessorInitializationContext initializationContext;


    private StandardPythonProcessorBridge(final Builder builder) {
        this.controller = builder.controller;
        this.creationWorkflow = builder.creationWorkflow;
        this.processorDetails = builder.processorDetails;
        this.workingDir = builder.workDir;
        this.moduleFile = builder.moduleFile;
        this.lastModified = this.moduleFile.lastModified();

        this.proxy = createProxy();
    }

    @Override
    public Optional<PythonProcessorAdapter> getProcessorAdapter() {
        return Optional.ofNullable(adapter);
    }

    @Override
    public Future<Void> initialize(final PythonProcessorInitializationContext context) {
        this.initializationContext = context;

        final String threadName = "Initialize Python Processor %s (%s)".formatted(initializationContext.getIdentifier(), getProcessorType());
        final CompletableFuture<Void> future = new CompletableFuture<>();

        Thread.ofVirtual().name(threadName).start(() -> initializePythonSide(future));
        return future;
    }

    public LoadState getLoadState() {
        return loadState;
    }

    private void initializePythonSide(final CompletableFuture<Void> future) {
        try {
            try {
                creationWorkflow.downloadDependencies();
                loadState = LoadState.LOADING_PROCESSOR_CODE;
            } catch (final Exception e) {
                loadState = LoadState.DEPENDENCY_DOWNLOAD_FAILED;
                throw e;
            }

            final PythonProcessorAdapter pythonProcessorAdapter;
            try {
                pythonProcessorAdapter = creationWorkflow.createProcessor();
                pythonProcessorAdapter.initialize(initializationContext);
                this.adapter = pythonProcessorAdapter;
                this.proxy.onPythonSideInitialized(pythonProcessorAdapter);

                loadState = LoadState.FINISHED_LOADING;
            } catch (final Exception e) {
                loadState = LoadState.LOADING_PROCESSOR_CODE_FAILED;
                throw e;
            }

            future.complete(null);
        } catch (final Throwable t) {
            future.completeExceptionally(t);
        }
    }

    @Override
    public String getProcessorType() {
        return processorDetails.getProcessorType();
    }

    @Override
    public Processor getProcessorProxy() {
        return proxy;
    }

    @Override
    public boolean reload() {
        if (moduleFile.lastModified() <= lastModified) {
            logger.debug("Processor {} has not been modified since it was last loaded so will not reload", getProcessorType());
            return false;
        }

        controller.reloadProcessor(getProcessorType(), processorDetails.getProcessorVersion(), workingDir.getAbsolutePath());
        initializePythonSide(new CompletableFuture<>());
        lastModified = moduleFile.lastModified();

        return true;
    }

    private PythonProcessorProxy createProxy() {
        final String implementedInterface = processorDetails.getInterface();
        if (FlowFileTransform.class.getName().equals(implementedInterface)) {
            return new FlowFileTransformProxy(this);
        }
        if (RecordTransform.class.getName().equals(implementedInterface)) {
            return new RecordTransformProxy(this);
        }

        throw new IllegalArgumentException("Python Processor does not implement any of the valid interfaces. Interface implemented: " + implementedInterface);
    }


    public static class Builder {
        private PythonController controller;
        private ProcessorCreationWorkflow creationWorkflow;
        private File workDir;
        private File moduleFile;
        private PythonProcessorDetails processorDetails;

        public Builder controller(final PythonController controller) {
            this.controller = controller;
            return this;
        }

        public Builder creationWorkflow(final ProcessorCreationWorkflow creationWorkflow) {
            this.creationWorkflow = creationWorkflow;
            return this;
        }

        public Builder processorDetails(final PythonProcessorDetails details) {
            this.processorDetails = details;
            return this;
        }

        public Builder workingDirectory(final File workDir) {
            this.workDir = workDir;
            return this;
        }

        public Builder moduleFile(final File moduleFile) {
            this.moduleFile = moduleFile;
            return this;
        }

        public StandardPythonProcessorBridge build() {
            if (controller == null) {
                throw new IllegalStateException("Must specify the PythonController");
            }
            if (creationWorkflow == null) {
                throw new IllegalStateException("Must specify the Processor Creation Workflow");
            }
            if (processorDetails == null) {
                throw new IllegalStateException("Must specify the Processor Details");
            }
            if (workDir == null) {
                throw new IllegalStateException("Must specify the Working Directory");
            }
            if (moduleFile == null) {
                throw new IllegalStateException("Must specify the Module File");
            }

            return new StandardPythonProcessorBridge(this);
        }
    }
}
