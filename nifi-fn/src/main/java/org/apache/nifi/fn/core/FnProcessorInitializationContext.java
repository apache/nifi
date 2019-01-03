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
package org.apache.nifi.fn.core;

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;

import java.io.File;
import java.util.UUID;

public class FnProcessorInitializationContext implements ProcessorInitializationContext {
    private final ComponentLog logger;
    private final String processorId;
    private final ProcessContext context;

    public FnProcessorInitializationContext(final Processor processor, final ProcessContext context) {
        processorId = UUID.randomUUID().toString();
        logger = new SLF4JComponentLog(processor);
        this.context = context;
    }

    public String getIdentifier() {
        return processorId;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    public ControllerServiceLookup getControllerServiceLookup() {
        return context.getControllerServiceLookup();
    }

    public NodeTypeProvider getNodeTypeProvider() {
        return new NodeTypeProvider() {
            public boolean isClustered() {
                return false;
            }

            public boolean isPrimary() {
                return false;
            }
        };
    }

    public String getKerberosServicePrincipal() {
        return null; //this needs to be wired in.
    }

    public File getKerberosServiceKeytab() {
        return null; //this needs to be wired in.
    }

    public File getKerberosConfigurationFile() {
        return null; //this needs to be wired in.
    }
}
