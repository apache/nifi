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

import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.PythonProcessorInitializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState;

public class StandardPythonProcessorBridge implements PythonProcessorBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonProcessorBridge.class);

    private final ProcessorCreationWorkflow creationWorkflow;
    private final String processorType;
    private final String processorVersion;
    private volatile PythonProcessorAdapter adapter;
    private final File workingDir;
    private final File moduleFile;
    private volatile long lastModified;
    private volatile LoadState loadState = LoadState.DOWNLOADING_DEPENDENCIES;
    private volatile PythonProcessorInitializationContext initializationContext;
    private volatile String identifier;
    private volatile PythonController controller;
    private volatile CompletableFuture<Void> initializationFuture;


    private StandardPythonProcessorBridge(final Builder builder) {
        this.controller = builder.controller;
        this.creationWorkflow = builder.creationWorkflow;
        this.processorType = builder.processorType;
        this.processorVersion = builder.processorVersion;
        this.workingDir = builder.workDir;
        this.moduleFile = builder.moduleFile;
        this.lastModified = this.moduleFile.lastModified();
    }

    @Override
    public Optional<PythonProcessorAdapter> getProcessorAdapter() {
        return Optional.ofNullable(adapter);
    }

    @Override
    public void initialize(final PythonProcessorInitializationContext context) {
        if (initializationFuture != null) {
            initializationFuture.cancel(true);
        }

        this.initializationContext = context;
        this.identifier = context.getIdentifier();

        final CompletableFuture<Void> future = new CompletableFuture<>();
        this.initializationFuture = future;
        final String threadName = "Initialize Python Processor %s (%s)".formatted(identifier, getProcessorType());
        Thread.ofVirtual().name(threadName).start(() -> initializePythonSide(true, future));
    }

    @Override
    public void replaceController(final PythonController controller) {
        if (initializationFuture != null) {
            initializationFuture.cancel(true);
        }

        this.controller = controller;
        this.adapter = null;

        final CompletableFuture<Void> future = new CompletableFuture<>();
        this.initializationFuture = future;

        final String threadName = "Re-Initialize Python Processor %s (%s)".formatted(identifier, getProcessorType());
        Thread.ofVirtual().name(threadName).start(() -> initializePythonSide(true, future));
    }

    public LoadState getLoadState() {
        return loadState;
    }

    private void initializePythonSide(final boolean continualRetry, final CompletableFuture<Void> future) {
        if (initializationContext == null) {
            future.complete(null);
            return;
        }

        long sleepMillis = 1_000L;
        while (!future.isCancelled()) {
            final boolean packagedWithDependencies = creationWorkflow.isPackagedWithDependencies();
            if (packagedWithDependencies) {
                loadState = LoadState.LOADING_PROCESSOR_CODE;
                break;
            }

            loadState = LoadState.DOWNLOADING_DEPENDENCIES;

            try {
                creationWorkflow.downloadDependencies();
                logger.info("Successfully downloaded dependencies for Python Processor {} ({})", identifier, getProcessorType());

                break;
            } catch (final Exception e) {
                loadState = LoadState.DEPENDENCY_DOWNLOAD_FAILED;
                if (!continualRetry) {
                    throw e;
                }

                sleepMillis = Math.min(sleepMillis * 2, TimeUnit.MINUTES.toMillis(10));
                logger.error("Failed to download dependencies for Python Processor {} ({}). Will try again in {} millis", identifier, getProcessorType(), sleepMillis, e);

                try {
                    Thread.sleep(sleepMillis);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    e.addSuppressed(ex);
                    throw e;
                }
            }
        }

        while (!future.isCancelled()) {
            loadState = LoadState.LOADING_PROCESSOR_CODE;

            try {
                final PythonProcessorAdapter pythonProcessorAdapter = creationWorkflow.createProcessor();
                pythonProcessorAdapter.initialize(initializationContext);
                this.adapter = pythonProcessorAdapter;
                loadState = LoadState.FINISHED_LOADING;
                logger.info("Successfully loaded Python Processor {} ({})", identifier, getProcessorType());
                break;
            } catch (final Exception e) {
                loadState = LoadState.LOADING_PROCESSOR_CODE_FAILED;

                if (!continualRetry) {
                    throw e;
                }

                sleepMillis = Math.min(sleepMillis * 2, TimeUnit.MINUTES.toMillis(10));
                logger.error("Failed to load code for Python Processor {} ({}). Will try again in {} millis", identifier, getProcessorType(), sleepMillis, e);

                try {
                    Thread.sleep(sleepMillis);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    e.addSuppressed(ex);
                    throw e;
                }
            }
        }

        future.complete(null);
    }

    @Override
    public String getProcessorType() {
        return processorType;
    }

    @Override
    public boolean reload() {
        if (moduleFile.lastModified() <= lastModified) {
            logger.debug("Processor {} has not been modified since it was last loaded so will not reload", getProcessorType());
            return false;
        }

        controller.reloadProcessor(getProcessorType(), processorVersion, workingDir.getAbsolutePath());
        initializePythonSide(false, new CompletableFuture<>());
        lastModified = moduleFile.lastModified();

        return true;
    }


    public static class Builder {
        private PythonController controller;
        private ProcessorCreationWorkflow creationWorkflow;
        private File workDir;
        private File moduleFile;
        private String processorType;
        private String processorVersion;

        public Builder controller(final PythonController controller) {
            this.controller = controller;
            return this;
        }

        public Builder creationWorkflow(final ProcessorCreationWorkflow creationWorkflow) {
            this.creationWorkflow = creationWorkflow;
            return this;
        }

        public Builder processorType(final String processorType) {
            this.processorType = processorType;
            return this;
        }

        public Builder processorVersion(final String processorVersion) {
            this.processorVersion = processorVersion;
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
            if (processorType == null) {
                throw new IllegalStateException("Must specify the Processor Type");
            }
            if (processorVersion == null) {
                throw new IllegalStateException("Must specify the Processor Version");
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
