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

package org.apache.nifi.components.resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

public class FileResourceReference implements ResourceReference {
    private final File file;
    private final ResourceType resourceType;

    public FileResourceReference(final File file) {
        this.file = Objects.requireNonNull(file);
        this.resourceType = file.isDirectory() ? ResourceType.DIRECTORY : ResourceType.FILE;
    }

    @Override
    public File asFile() {
        return file;
    }

    @Override
    public URL asURL() {
        try {
            return file.toURI().toURL();
        } catch (final MalformedURLException e) {
            throw new AssertionError("File " + file.getAbsolutePath() + " cannot be represented as a URL"); // we won't encounter this.
        }
    }

    @Override
    public InputStream read() throws IOException {
        if (resourceType != ResourceType.FILE) {
            throw new FileNotFoundException("Could not read from file with name " + file.getAbsolutePath() + " because that references a directory");
        }

        return new FileInputStream(file);
    }

    @Override
    public boolean isAccessible() {
        return file.exists() && file.canRead();
    }

    @Override
    public String getLocation() {
        return file.getAbsolutePath();
    }

    @Override
    public ResourceType getResourceType() {
        return resourceType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FileResourceReference that = (FileResourceReference) o;
        return Objects.equals(file, that.file)
            && resourceType == that.resourceType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, resourceType);
    }

    @Override
    public String toString() {
        return "FileResourceReference[file=" + file + ", resourceType=" + resourceType + "]";
    }
}
