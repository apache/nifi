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
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.PythonProcessorInitializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;

import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState;

public class StandardPythonProcessorBridge implements PythonProcessorBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonProcessorBridge.class);

    private final PythonController controller;
    private final ProcessorCreationWorkflow creationWorkflow;
    private final PythonProcessorDetails processorDetails;
    private volatile PythonProcessorAdapter adapter;
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
    }

    @Override
    public Optional<PythonProcessorAdapter> getProcessorAdapter() {
        return Optional.ofNullable(adapter);
    }

    @Override
    public void initialize(final PythonProcessorInitializationContext context) {
        this.initializationContext = context;

        final String threadName = "Initialize Python Processor %s (%s)".formatted(initializationContext.getIdentifier(), getProcessorType());
        Thread.ofVirtual().name(threadName).start(this::initializePythonSide);
    }

    public LoadState getLoadState() {
        return loadState;
    }

    private void initializePythonSide() {
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
            loadState = LoadState.FINISHED_LOADING;
        } catch (final Exception e) {
            loadState = LoadState.LOADING_PROCESSOR_CODE_FAILED;
            throw e;
        }
    }

    @Override
    public String getProcessorType() {
        return processorDetails.getProcessorType();
    }

    @Override
    public boolean reload() {
        if (moduleFile.lastModified() <= lastModified) {
            logger.debug("Processor {} has not been modified since it was last loaded so will not reload", getProcessorType());
            return false;
        }

        controller.reloadProcessor(getProcessorType(), processorDetails.getProcessorVersion(), workingDir.getAbsolutePath());
        initializePythonSide();
        lastModified = moduleFile.lastModified();

        return true;
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
