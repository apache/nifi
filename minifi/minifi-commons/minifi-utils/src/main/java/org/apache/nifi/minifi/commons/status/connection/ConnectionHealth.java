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

package org.apache.nifi.minifi.commons.status.connection;

public class ConnectionHealth implements java.io.Serializable {
    private int queuedCount;
    private long queuedBytes;

    public ConnectionHealth() {
    }

    public int getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(int queuedCount) {
        this.queuedCount = queuedCount;
    }

    public long getQueuedBytes() {
        return queuedBytes;
    }

    public void setQueuedBytes(long queuedBytes) {
        this.queuedBytes = queuedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionHealth that = (ConnectionHealth) o;

        if (getQueuedCount() != that.getQueuedCount()) return false;
        return getQueuedBytes() == that.getQueuedBytes();

    }

    @Override
    public int hashCode() {
        int result = getQueuedCount();
        result = 31 * result + (int) (getQueuedBytes() ^ (getQueuedBytes() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "queuedCount=" + queuedCount +
                ", queuedBytes=" + queuedBytes +
                '}';
    }
}
