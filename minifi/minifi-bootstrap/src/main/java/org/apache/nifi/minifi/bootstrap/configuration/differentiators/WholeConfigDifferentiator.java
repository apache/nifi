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

package org.apache.nifi.minifi.bootstrap.configuration.differentiators;

import static java.util.Optional.ofNullable;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WholeConfigDifferentiator {

    public static final String WHOLE_CONFIG_KEY = "Whole Config";

    private final static Logger logger = LoggerFactory.getLogger(WholeConfigDifferentiator.class);

    protected volatile ConfigurationFileHolder configurationFileHolder;

    public void initialize(ConfigurationFileHolder configurationFileHolder) {
        this.configurationFileHolder = configurationFileHolder;
    }

    public static class ByteBufferInputDifferentiator extends WholeConfigDifferentiator implements Differentiator<ByteBuffer> {
        public boolean isNew(ByteBuffer newFlowConfig) {
            AtomicReference<ByteBuffer> currentFlowConfigReference = configurationFileHolder.getConfigFileReference();
            ByteBuffer currentFlowConfig = currentFlowConfigReference.get();
            boolean compareResult = ofNullable(currentFlowConfig)
                .map(newFlowConfig::compareTo)
                .map(result -> result != 0)
                .orElse(Boolean.TRUE);
            logger.debug("New flow is different from existing flow: {}", compareResult);
            return compareResult;
        }
    }

    public static Differentiator<ByteBuffer> getByteBufferDifferentiator() {
        return new ByteBufferInputDifferentiator();
    }
}
