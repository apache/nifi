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

package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Provides a wrapper for the elements that make an extension unique. For example, there may be a class named org.apache.nifi.extensions.ExtensionABC
 * that exists in a given NiFi Archive (NAR). There may also be several classes named org.apache.nifi.extensions.ExtensionABC, each in its own NAR,
 * and each of those would be a separate extension. This class provides a mechanism by which the relevant bits to determine an extension's uniqueness
 * can be bundled together into a single class.
 */
public class ExtensionDefinition {
    private final String implementationClassName;
    private final Bundle bundle;
    private final Class<?> extensionType;
    private final String description;
    private final List<String> tags;
    private final String version;
    private final ExtensionRuntime runtime;

    private ExtensionDefinition(final Builder builder) {
        this.implementationClassName = builder.implementationClassName;
        this.bundle = builder.bundle;
        this.extensionType = builder.extensionType;
        this.description = builder.description;
        this.tags = builder.tags;
        this.version = builder.version;
        this.runtime = builder.runtime;
    }

    /**
     * @return the fully qualified class name of the class that implements the extension
     */
    public String getImplementationClassName() {
        return implementationClassName;
    }

    /**
     * @return the Bundle that contains the extension
     */
    public Bundle getBundle() {
        return bundle;
    }

    /**
     * @return the type of Extension (e.g., {@link org.apache.nifi.processor.Processor}, {@link org.apache.nifi.controller.ControllerService},
     * {@link org.apache.nifi.parameter.ParameterProvider}, {@link org.apache.nifi.reporting.ReportingTask} or {@link org.apache.nifi.flowanalysis.FlowAnalysisRule}.
     */
    public Class<?> getExtensionType() {
        return extensionType;
    }

    /**
     * @return the capability description for extension
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the extension's tags
     */
    public List<String> getTags() {
        return tags;
    }

    public String getVersion() {
        return version;
    }

    public ExtensionRuntime getRuntime() {
        return runtime;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ExtensionDefinition that = (ExtensionDefinition) o;
        return Objects.equals(implementationClassName, that.implementationClassName)
            && Objects.equals(bundle, that.bundle)
            && Objects.equals(extensionType, that.extensionType)
            && Objects.equals(version, that.version)
            && Objects.equals(runtime, that.runtime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(implementationClassName, bundle, extensionType, version, runtime);
    }

    @Override
    public String toString() {
        return "ExtensionDefinition[type=" + extensionType.getSimpleName() + ", implementation=" + implementationClassName + ", bundle=" + bundle + "]";
    }


    public static class Builder {
        private String implementationClassName;
        private Bundle bundle;
        private Class<?> extensionType;
        private String description;
        private List<String> tags;
        private String version;
        private ExtensionRuntime runtime;

        public Builder implementationClassName(final String className) {
            this.implementationClassName = className;
            return this;
        }

        public Builder runtime(final ExtensionRuntime runtime) {
            this.runtime = runtime;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder bundle(final Bundle bundle) {
            this.bundle = bundle;
            return this;
        }

        public Builder extensionType(final Class<?> extensionType) {
            this.extensionType = extensionType;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder tags(final List<String> tags) {
            this.tags = new ArrayList<>(tags);
            return this;
        }

        public ExtensionDefinition build() {
            requireSet(implementationClassName, "Implementation Class Name");
            requireSet(extensionType, "Extension Type");
            requireSet(bundle, "Bundle");

            return new ExtensionDefinition(this);
        }

        private void requireSet(final Object value, final String fieldName) {
            if (value == null) {
                throw new IllegalArgumentException(fieldName + " must be specified");
            }
        }
    }

    public enum ExtensionRuntime {
        JAVA,

        PYTHON;
    }
}
