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

package org.apache.nifi.minifi.commons.status.system;

public class ContentRepositoryUsage implements java.io.Serializable {

    private String name;
    private long freeSpace;
    private long totalSpace;
    private long usedSpace;
    private int diskUtilization;

    public ContentRepositoryUsage() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(long totalSpace) {
        this.totalSpace = totalSpace;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }

    public int getDiskUtilization() {
        return diskUtilization;
    }

    public void setDiskUtilization(int diskUtilization) {
        this.diskUtilization = diskUtilization;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContentRepositoryUsage that = (ContentRepositoryUsage) o;

        if (getFreeSpace() != that.getFreeSpace()) return false;
        if (getTotalSpace() != that.getTotalSpace()) return false;
        if (getUsedSpace() != that.getUsedSpace()) return false;
        if (getDiskUtilization() != that.getDiskUtilization()) return false;
        return getName() != null ? getName().equals(that.getName()) : that.getName() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (int) (getFreeSpace() ^ (getFreeSpace() >>> 32));
        result = 31 * result + (int) (getTotalSpace() ^ (getTotalSpace() >>> 32));
        result = 31 * result + (int) (getUsedSpace() ^ (getUsedSpace() >>> 32));
        result = 31 * result + getDiskUtilization();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", freeSpace=" + freeSpace +
                ", totalSpace=" + totalSpace +
                ", usedSpace=" + usedSpace +
                ", diskUtilization=" + diskUtilization +
                '}';
    }
}
