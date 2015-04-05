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
package org.apache.nifi.web;

/**
 *
 */
@Deprecated
public class ProcessorConfigurationAction {

    private final String processorId;
    private final String processorName;
    private final String processorType;
    private final String name;
    private final String previousValue;
    private final String value;

    private ProcessorConfigurationAction(final Builder builder) {
        this.processorId = builder.processorId;
        this.processorName = builder.processorName;
        this.processorType = builder.processorType;
        this.name = builder.name;
        this.previousValue = builder.previousValue;
        this.value = builder.value;
    }

    /**
     * Gets the id of the processor.
     *
     * @return
     */
    public String getProcessorId() {
        return processorId;
    }

    /**
     * Gets the name of the processor being modified.
     *
     * @return
     */
    public String getProcessorName() {
        return processorName;
    }

    /**
     * Gets the type of the processor being modified.
     *
     * @return
     */
    public String getProcessorType() {
        return processorType;
    }

    /**
     * Gets the name of the field, property, etc that has been modified.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the previous value.
     *
     * @return
     */
    public String getPreviousValue() {
        return previousValue;
    }

    /**
     * Gets the new value.
     *
     * @return
     */
    public String getValue() {
        return value;
    }

    public static class Builder {

        private String processorId;
        private String processorName;
        private String processorType;
        private String name;
        private String previousValue;
        private String value;

        public Builder processorId(final String processorId) {
            this.processorId = processorId;
            return this;
        }

        public Builder processorName(final String processorName) {
            this.processorName = processorName;
            return this;
        }

        public Builder processorType(final String processorType) {
            this.processorType = processorType;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder previousValue(final String previousValue) {
            this.previousValue = previousValue;
            return this;
        }

        public Builder value(final String value) {
            this.value = value;
            return this;
        }

        public ProcessorConfigurationAction build() {
            return new ProcessorConfigurationAction(this);
        }
    }
}
