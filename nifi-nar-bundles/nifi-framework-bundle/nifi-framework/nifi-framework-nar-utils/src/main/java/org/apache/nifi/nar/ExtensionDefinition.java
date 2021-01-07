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

public class ExtensionDefinition {
    private final String implementationClassName;
    private final Bundle bundle;
    private final Class<?> extensionType;

    public ExtensionDefinition(final String implementationClassName, final Bundle bundle, final Class<?> extensionType) {
        this.implementationClassName = implementationClassName;
        this.bundle = bundle;
        this.extensionType = extensionType;
    }

    public String getImplementationClassName() {
        return implementationClassName;
    }

    public Bundle getBundle() {
        return bundle;
    }

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
        return Objects.equals(implementationClassName, that.implementationClassName) &&
            Objects.equals(bundle, that.bundle) &&
            Objects.equals(extensionType, that.extensionType);
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
