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

/**
 * Holds a singleton instance of ExtensionManager.
 *
 * NOTE: This class primarily exists to create a bridge from the JettyServer to the Spring context. There
 * should be no direct calls to this class outside of the JettyServer or the ExtensionManagerFactoryBean.
 * The rest of the framework should obtain an instance of ExtensionManager from Spring.
 */
public final class ExtensionManagerHolder {

    private static volatile ExtensionManager INSTANCE;

    public static void init(final ExtensionManager extensionManager) {
        if (INSTANCE == null) {
            synchronized (ExtensionManagerHolder.class) {
                if (INSTANCE == null) {
                    INSTANCE = extensionManager;
                } else {
                    throw new IllegalStateException("Cannot reinitialize ExtensionManagerHolder");
                }
            }
        } else {
            throw new IllegalStateException("Cannot reinitialize ExtensionManagerHolder");
        }
    }

    public static ExtensionManager getExtensionManager() {
        if (INSTANCE == null) {
            synchronized (ExtensionManagerHolder.class) {
                if (INSTANCE == null) {
                    throw new IllegalStateException("ExtensionManagerHolder was never initialized");
                }
            }
        }

        return INSTANCE;
    }

    // Private access
    private ExtensionManagerHolder() {

    }

}
