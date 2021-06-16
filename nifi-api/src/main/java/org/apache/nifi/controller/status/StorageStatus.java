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
package org.apache.nifi.controller.status;

/**
 * The status of a storage repository.
 */
public class StorageStatus implements Cloneable {
    private String name;
    private long freeSpace;
    private long usedSpace;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(final long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(final long usedSpace) {
        this.usedSpace = usedSpace;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("StorageStatus{");
        builder.append("name='").append(name).append('\'');
        builder.append(", freeSpace=").append(freeSpace);
        builder.append(", usedSpace=").append(usedSpace);
        builder.append('}');
        return builder.toString();
    }

    @Override
    public StorageStatus clone() {
        final StorageStatus clonedObj = new StorageStatus();
        clonedObj.name = name;
        clonedObj.freeSpace = freeSpace;
        clonedObj.usedSpace = usedSpace;
        return clonedObj;
    }
}
