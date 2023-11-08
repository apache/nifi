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

import org.apache.nifi.minifi.commons.status.common.AbstractStatusBean;

public class ConnectionStatusBean extends AbstractStatusBean {

    private ConnectionHealth connectionHealth;
    private ConnectionStats connectionStats;

    public ConnectionHealth getConnectionHealth() {
        return connectionHealth;
    }

    public void setConnectionHealth(ConnectionHealth connectionHealth) {
        this.connectionHealth = connectionHealth;
    }

    public ConnectionStats getConnectionStats() {
        return connectionStats;
    }

    public void setConnectionStats(ConnectionStats connectionStats) {
        this.connectionStats = connectionStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionStatusBean that = (ConnectionStatusBean) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        if (getConnectionHealth() != null ? !getConnectionHealth().equals(that.getConnectionHealth()) : that.getConnectionHealth() != null) return false;
        return getConnectionStats() != null ? getConnectionStats().equals(that.getConnectionStats()) : that.getConnectionStats() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getConnectionHealth() != null ? getConnectionHealth().hashCode() : 0);
        result = 31 * result + (getConnectionStats() != null ? getConnectionStats().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "id='" + getId() + '\'' +
                "name='" + getName() + '\'' +
                ", connectionHealth=" + connectionHealth +
                ", connectionStats=" + connectionStats +
                '}';
    }
}
