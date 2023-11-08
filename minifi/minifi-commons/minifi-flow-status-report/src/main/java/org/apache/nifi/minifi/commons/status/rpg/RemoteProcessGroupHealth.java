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

public class RemoteProcessGroupHealth implements java.io.Serializable {
    private String transmissionStatus;
    private boolean hasBulletins;
    private int activePortCount;
    private int inactivePortCount;

    public RemoteProcessGroupHealth() {
    }

    public String getTransmissionStatus() {
        return transmissionStatus;
    }

    public void setTransmissionStatus(String transmissionStatus) {
        this.transmissionStatus = transmissionStatus;
    }

    public boolean isHasBulletins() {
        return hasBulletins;
    }

    public void setHasBulletins(boolean hasBulletins) {
        this.hasBulletins = hasBulletins;
    }

    public int getActivePortCount() {
        return activePortCount;
    }

    public void setActivePortCount(int activePortCount) {
        this.activePortCount = activePortCount;
    }

    public int getInactivePortCount() {
        return inactivePortCount;
    }

    public void setInactivePortCount(int inactivePortCount) {
        this.inactivePortCount = inactivePortCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteProcessGroupHealth that = (RemoteProcessGroupHealth) o;

        if (isHasBulletins() != that.isHasBulletins()) return false;
        if (getActivePortCount() != that.getActivePortCount()) return false;
        if (getInactivePortCount() != that.getInactivePortCount()) return false;
        return getTransmissionStatus() != null ? getTransmissionStatus().equals(that.getTransmissionStatus()) : that.getTransmissionStatus() == null;

    }

    @Override
    public int hashCode() {
        int result = getTransmissionStatus() != null ? getTransmissionStatus().hashCode() : 0;
        result = 31 * result + (isHasBulletins() ? 1 : 0);
        result = 31 * result + getActivePortCount();
        result = 31 * result + getInactivePortCount();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "transmissionStatus='" + transmissionStatus + '\'' +
                ", hasBulletins=" + hasBulletins +
                ", activePortCount=" + activePortCount +
                ", inactivePortCount=" + inactivePortCount +
                '}';
    }
}
