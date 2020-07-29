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

package org.apache.nifi.provenance.index.lucene;

import java.io.File;

public class IndexLocation {
    private final File indexDirectory;
    private final long indexStartTimestamp;
    private final String partitionName;

    public IndexLocation(final File indexDirectory, final long indexStartTimestamp, final String partitionName) {
        this.indexDirectory = indexDirectory;
        this.indexStartTimestamp = indexStartTimestamp;
        this.partitionName = partitionName;
    }

    public File getIndexDirectory() {
        return indexDirectory;
    }

    public long getIndexStartTimestamp() {
        return indexStartTimestamp;
    }

    public long getIndexEndTimestamp() {
        final long lastMod = indexDirectory.lastModified();
        if (lastMod == 0) {
            return System.currentTimeMillis();
        }

        return lastMod;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * indexDirectory.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof IndexLocation)) {
            return false;
        }

        final IndexLocation other = (IndexLocation) obj;
        return indexDirectory.equals(other.getIndexDirectory());
    }

    @Override
    public String toString() {
        return "IndexLocation[directory=" + indexDirectory + "]";
    }
}
