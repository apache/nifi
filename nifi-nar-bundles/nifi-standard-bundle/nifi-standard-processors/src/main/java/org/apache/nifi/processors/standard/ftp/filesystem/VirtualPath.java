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
package org.apache.nifi.processors.standard.ftp.filesystem;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class VirtualPath {

    private final Path path; // always normalized

    public VirtualPath(String path) {
        String absolutePath = File.separator + normalizeSeparator(path);
        this.path = Paths.get(absolutePath).normalize();
    }

    private String normalizeSeparator(String path) {
        String pathWithoutStartingSeparator = removeStartingSeparator(path);
        String normalizedPath = pathWithoutStartingSeparator.replace(File.separatorChar, '/');
        normalizedPath = normalizedPath.replace('\\', '/');
        return normalizedPath;
    }

    private String removeStartingSeparator(String path) {
        int indexOfFirstNonSeparator;
        for (indexOfFirstNonSeparator = 0; indexOfFirstNonSeparator < path.length(); ++indexOfFirstNonSeparator) {
            if (!(path.charAt(indexOfFirstNonSeparator) == File.separatorChar) && !(path.charAt(indexOfFirstNonSeparator) == '/')) {
                break;
            }
        }
        return path.substring(indexOfFirstNonSeparator);
    }

    public String getFileName() {
        if (path.getFileName() == null) {
            return File.separator;
        } else {
            return path.getFileName().toString();
        }
    }

    public VirtualPath getParent() {
        if (path.getParent() == null) {
            return null;
        } else {
            return new VirtualPath(path.getParent().toString());
        }
    }

    public boolean isAbsolute() {
        return path.isAbsolute();
    }

    public VirtualPath resolve(String otherPath) {
        return new VirtualPath(path.resolve(otherPath).normalize().toString());
    }

    public String toString() {
        return path.toString();
    }

    public int getNameCount() {
        return path.getNameCount();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof VirtualPath)) {
            return false;
        }
        VirtualPath other = (VirtualPath) o;
        return path.equals(other.path);
    }
}
