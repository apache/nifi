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

package org.apache.nifi.c2.protocol.component.api;


import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;

public class ProcessorConfiguration implements Serializable {
    private String processorClassName;
    private String configuration;

    @Schema(description = "The fully qualified classname of the Processor that should be used to accomplish the use case")
    public String getProcessorClassName() {
        return processorClassName;
    }

    public void setProcessorClassName(final String processorClassName) {
        this.processorClassName = processorClassName;
    }

    @Schema(description = "A description of how the Processor should be configured in order to accomplish the use case")
    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(final String configuration) {
        this.configuration = configuration;
    }
}
