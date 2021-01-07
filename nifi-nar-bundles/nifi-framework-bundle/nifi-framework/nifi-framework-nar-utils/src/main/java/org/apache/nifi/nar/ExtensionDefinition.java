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

    public ExtensionDefinition(final String implementationClassName, final Bundle bundle, final Class<?> extensionType) {
        this.implementationClassName = implementationClassName;
        this.bundle = bundle;
        this.extensionType = extensionType;
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
     * or {@link org.apache.nifi.reporting.ReportingTask}.
     */
    public Class<?> getExtensionType() {
        return extensionType;
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
            && Objects.equals(extensionType, that.extensionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(implementationClassName, bundle, extensionType);
    }

    @Override
    public String toString() {
        return "ExtensionDefinition[type=" + extensionType.getSimpleName() + ", implementation=" + implementationClassName + ", bundle=" + bundle + "]";
    }
}
