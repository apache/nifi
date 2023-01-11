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
package org.apache.nifi.nar.provider;

import org.apache.nifi.flow.resource.ExternalResourceDescriptor;
import org.apache.nifi.flow.resource.ExternalResourceProvider;
import org.apache.nifi.flow.resource.ExternalResourceProviderInitializationContext;
import org.apache.nifi.flow.resource.ImmutableExternalResourceDescriptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class LocalDirectoryNarProvider implements ExternalResourceProvider {

    private static final String SOURCE_DIR_PROPERTY = "source.dir";

    private File sourceDir;

    @Override
    public void initialize(final ExternalResourceProviderInitializationContext context) {
        final String sourceDirValue = context.getProperties().get(SOURCE_DIR_PROPERTY);
        if (sourceDirValue == null) {
            throw new IllegalStateException(SOURCE_DIR_PROPERTY + " is required");
        }

        sourceDir = new File(sourceDirValue);
        if (!sourceDir.exists()) {
            throw new IllegalStateException("Local NAR directory does not exist at: " + sourceDir.getAbsolutePath());
        }
    }

    @Override
    public Collection<ExternalResourceDescriptor> listResources() throws IOException {
        return Arrays.stream(sourceDir.listFiles((dir, name) -> name.endsWith(".nar")))
                .map(this::createDescriptor)
                .collect(Collectors.toList());
    }

    private ExternalResourceDescriptor createDescriptor(final File file) {
        return new ImmutableExternalResourceDescriptor(file.getName(), file.lastModified());
    }

    @Override
    public InputStream fetchExternalResource(final ExternalResourceDescriptor descriptor) throws IOException {
        final File file = new File(sourceDir, descriptor.getLocation());
        return new FileInputStream(file);
    }
}
