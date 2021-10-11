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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;

public final class StatusHistoryRepositoryFactory {

    private StatusHistoryRepositoryFactory() {
        // factory class, not to instantiate
    }

    private static final String DEFAULT_COMPONENT_STATUS_REPO_IMPLEMENTATION = "org.apache.nifi.controller.status.history.VolatileComponentStatusRepository";

    public static StatusHistoryRepository createStatusHistoryRepositoryFactory(final NiFiProperties nifiProperties, final ExtensionManager extensionManager) {
        final String implementationClassName = nifiProperties.getProperty(NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION, DEFAULT_COMPONENT_STATUS_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new IllegalArgumentException(
                    String.format("Cannot create Status History Repository because the NiFi Properties is missing the following property: %s",
                     NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION)
            );
        }

        try {
            final StatusHistoryRepository statusHistoryRepository = NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName,
                    StatusHistoryRepository.class, nifiProperties);
            statusHistoryRepository.start();
            return statusHistoryRepository;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
