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

package org.apache.nifi.minifi.commons.status.rpg;

public class RemoteProcessGroupStats implements java.io.Serializable {
    private int activeThreads;
    private int sentCount;
    private long sentContentSize;

    public RemoteProcessGroupStats() {
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    public void setActiveThreads(int activeThreads) {
        this.activeThreads = activeThreads;
    }

    public int getSentCount() {
        return sentCount;
    }

    public void setSentCount(int sentCount) {
        this.sentCount = sentCount;
    }

    public long getSentContentSize() {
        return sentContentSize;
    }

    public void setSentContentSize(long sentContentSize) {
        this.sentContentSize = sentContentSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteProcessGroupStats that = (RemoteProcessGroupStats) o;

        if (getActiveThreads() != that.getActiveThreads()) return false;
        if (getSentCount() != that.getSentCount()) return false;
        return getSentContentSize() == that.getSentContentSize();

    }

    @Override
    public int hashCode() {
        int result = getActiveThreads();
        result = 31 * result + getSentCount();
        result = 31 * result + (int) (getSentContentSize() ^ (getSentContentSize() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "activeThreads=" + activeThreads +
                ", sentCount=" + sentCount +
                ", sentContentSize=" + sentContentSize +
                '}';
    }
}
