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
package org.apache.nifi.bundle;

/**
 * Represents a bundle that contains one or more extensions.
 */
public class Bundle {

    private final BundleDetails bundleDetails;

    private final ClassLoader classLoader;

    public Bundle(final BundleDetails bundleDetails, final ClassLoader classLoader) {
        this.bundleDetails = bundleDetails;
        this.classLoader = classLoader;

        if (this.bundleDetails == null) {
            throw new IllegalStateException("BundleDetails cannot be null");
        }

        if (this.classLoader == null) {
            throw new IllegalStateException("ClassLoader cannot be null");
        }
    }

    public BundleDetails getBundleDetails() {
        return bundleDetails;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
