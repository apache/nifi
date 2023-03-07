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
package org.apache.nifi.kubernetes.client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Service Account Namespace Provider based on standard file location
 */
public class ServiceAccountNamespaceProvider implements NamespaceProvider {
    protected static final String NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

    protected static final String DEFAULT_NAMESPACE = "default";

    /**
     * Get Namespace from Service Account location or return default namespace when not found
     *
     * @return Kubernetes Namespace
     */
    @Override
    public String getNamespace() {
        final Path namespacePath = Paths.get(NAMESPACE_PATH);
        return Files.isReadable(namespacePath) ? getNamespace(namespacePath) : DEFAULT_NAMESPACE;
    }

    private String getNamespace(final Path namespacePath) {
        try {
            final byte[] bytes = Files.readAllBytes(namespacePath);
            return new String(bytes, StandardCharsets.UTF_8).trim();
        } catch (final IOException e) {
            throw new UncheckedIOException("Read Service Account namespace failed", e);
        }
    }
}
