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
package org.apache.nifi.processors.standard.util;

import java.io.Serializable;

import org.apache.nifi.processor.util.list.ListableEntity;

public class FileInfo implements Comparable<FileInfo>, Serializable, ListableEntity {

    private static final long serialVersionUID = 1L;

    private final boolean directory;
    private final long size;
    private final long lastModifiedTime;
    private final String fileName;
    private final String fullPathFileName;
    private final String permissions;
    private final String owner;
    private final String group;

    public String getFileName() {
        return fileName;
    }

    public String getFullPathFileName() {
        return fullPathFileName;
    }

    public boolean isDirectory() {
        return directory;
    }

    public long getSize() {
        return size;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getPermissions() {
        return permissions;
    }

    public String getOwner() {
        return owner;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fullPathFileName == null) ? 0 : fullPathFileName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FileInfo other = (FileInfo) obj;
        if (fullPathFileName == null) {
            if (other.fullPathFileName != null) {
                return false;
            }
        } else if (!fullPathFileName.equals(other.fullPathFileName)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(FileInfo o) {
        return fullPathFileName.compareTo(o.fullPathFileName);
    }

    protected FileInfo(final Builder builder) {
        this.directory = builder.directory;
        this.size = builder.size;
        this.lastModifiedTime = builder.lastModifiedTime;
        this.fileName = builder.fileName;
        this.fullPathFileName = builder.fullPathFileName;
        this.permissions = builder.permissions;
        this.owner = builder.owner;
        this.group = builder.group;
    }

    public static final class Builder {

        private boolean directory;
        private long size;
        private long lastModifiedTime;
        private String fileName;
        private String fullPathFileName;
        private String permissions;
        private String owner;
        private String group;

        public FileInfo build() {
            return new FileInfo(this);
        }

        public Builder directory(boolean directory) {
            this.directory = directory;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder lastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }

        public Builder filename(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder fullPathFileName(String pathFileName) {
            this.fullPathFileName = pathFileName;
            return this;
        }

        public Builder permissions(String permissions) {
            this.permissions = permissions;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }
    }

    @Override
    public String getName() {
        return getFileName();
    }

    @Override
    public String getIdentifier() {
        final String fullPathName = getFullPathFileName();
        return fullPathName == null ? getName() : fullPathName;
    }

    @Override
    public long getTimestamp() {
        return getLastModifiedTime();
    }
}
