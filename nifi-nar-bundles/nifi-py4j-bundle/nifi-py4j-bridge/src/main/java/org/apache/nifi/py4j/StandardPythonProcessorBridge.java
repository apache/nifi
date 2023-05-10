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
import org.apache.nifi.python.processor.FlowFileTransform;
import org.apache.nifi.python.processor.FlowFileTransformProxy;
import org.apache.nifi.python.processor.PythonProcessor;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.PythonProcessorInitializationContext;
import org.apache.nifi.python.processor.PythonProcessorProxy;
import org.apache.nifi.python.processor.RecordTransformProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class StandardPythonProcessorBridge implements PythonProcessorBridge {
    private static final Logger logger = LoggerFactory.getLogger(StandardPythonProcessorBridge.class);

    private final PythonController controller;
    private volatile PythonProcessorAdapter adapter;
    private volatile Processor proxy;
    private final String processorType;
    private final String version;
    private final File workingDir;
    private final File moduleFile;
    private volatile long lastModified;
    private volatile PythonProcessorInitializationContext initializationContext;

    private StandardPythonProcessorBridge(final Builder builder) {
        this.controller = builder.controller;
        this.adapter = builder.adapter;
        this.processorType = builder.processorType;
        this.version = builder.version;
        this.workingDir = builder.workDir;
        this.moduleFile = builder.moduleFile;
        this.lastModified = this.moduleFile.lastModified();

        this.proxy = createProxy();
    }

    @Override
    public PythonProcessorAdapter getProcessorAdapter() {
        return adapter;
    }

    @Override
    public void initialize(final PythonProcessorInitializationContext context) {
        this.initializationContext = context;
        this.adapter.initialize(context);
    }

    @Override
    public String getProcessorType() {
        return processorType;
    }

    @Override
    public Processor getProcessorProxy() {
        return proxy;
    }

    @Override
    public boolean reload() {
        if (moduleFile.lastModified() <= lastModified) {
            logger.debug("Processor {} has not been modified since it was last loaded so will not reload", processorType);
            return false;
        }

        controller.reloadProcessor(processorType, version, workingDir.getAbsolutePath());
        adapter = controller.createProcessor(processorType, version, workingDir.getAbsolutePath());
        adapter.initialize(initializationContext);
        proxy = createProxy();
        lastModified = moduleFile.lastModified();

        return true;
    }

    private PythonProcessorProxy createProxy() {
        final PythonProcessor pythonProcessor = adapter.getProcessor();

        // Instantiate the appropriate Processor Proxy based on the type of Python Processor
        if (pythonProcessor instanceof FlowFileTransform) {
            return new FlowFileTransformProxy(this);
        } else {
            return new RecordTransformProxy(this);
        }
    }


    public static class Builder {
        private PythonController controller;
        private PythonProcessorAdapter adapter;
        private String processorType;
        private String version;
        private File workDir;
        private File moduleFile;

        public Builder controller(final PythonController controller) {
            this.controller = controller;
            return this;
        }

        public Builder processorAdapter(final PythonProcessorAdapter adapter) {
            this.adapter = adapter;
            return this;
        }

        public Builder processorType(final String type) {
            this.processorType = type;
            return this;
        }

        public Builder processorVersion(final String version) {
            this.version = version;
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
            if (adapter == null) {
                throw new IllegalStateException("Must specify the Processor Adapter");
            }
            if (processorType == null) {
                throw new IllegalStateException("Must specify the Processor Type");
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
