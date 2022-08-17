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

public class PortStatus implements java.io.Serializable {
    private String name;
    private boolean targetExists;
    private boolean targetRunning;

    public PortStatus() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isTargetExists() {
        return targetExists;
    }

    public void setTargetExists(boolean targetExists) {
        this.targetExists = targetExists;
    }

    public boolean isTargetRunning() {
        return targetRunning;
    }

    public void setTargetRunning(boolean targetRunning) {
        this.targetRunning = targetRunning;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PortStatus inputPortStatus = (PortStatus) o;

        if (isTargetExists() != inputPortStatus.isTargetExists()) return false;
        if (isTargetRunning() != inputPortStatus.isTargetRunning()) return false;
        return getName() != null ? getName().equals(inputPortStatus.getName()) : inputPortStatus.getName() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (isTargetExists() ? 1 : 0);
        result = 31 * result + (isTargetRunning() ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", targetExists=" + targetExists +
                ", targetRunning=" + targetRunning +
                '}';
    }
}
