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

package org.apache.nifi.minifi.commons.status.instance;

public class InstanceHealth implements java.io.Serializable {

    private int queuedCount;
    private double queuedContentSize;
    private boolean hasBulletins;
    private int activeThreads;

    public InstanceHealth() {
    }

    public int getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(int queuedCount) {
        this.queuedCount = queuedCount;
    }

    public double getQueuedContentSize() {
        return queuedContentSize;
    }

    public void setQueuedContentSize(double queuedContentSize) {
        this.queuedContentSize = queuedContentSize;
    }

    public boolean isHasBulletins() {
        return hasBulletins;
    }

    public void setHasBulletins(boolean hasBulletins) {
        this.hasBulletins = hasBulletins;
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    public void setActiveThreads(int activeThreads) {
        this.activeThreads = activeThreads;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstanceHealth that = (InstanceHealth) o;

        if (getQueuedCount() != that.getQueuedCount()) return false;
        if (Double.compare(that.getQueuedContentSize(), getQueuedContentSize()) != 0) return false;
        if (isHasBulletins() != that.isHasBulletins()) return false;
        return activeThreads == that.activeThreads;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = getQueuedCount();
        temp = Double.doubleToLongBits(getQueuedContentSize());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (isHasBulletins() ? 1 : 0);
        result = 31 * result + activeThreads;
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "queuedCount=" + queuedCount +
                ", queuedContentSize=" + queuedContentSize +
                ", hasBulletins=" + hasBulletins +
                ", activeThreads=" + activeThreads +
                '}';
    }
}
