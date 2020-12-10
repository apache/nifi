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

public class SystemProcessorStats implements java.io.Serializable {

    private double loadAverage;
    private int availableProcessors;

    public SystemProcessorStats() {
    }

    public double getLoadAverage() {
        return loadAverage;
    }

    public void setLoadAverage(double loadAverage) {
        this.loadAverage = loadAverage;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SystemProcessorStats that = (SystemProcessorStats) o;

        if (Double.compare(that.getLoadAverage(), getLoadAverage()) != 0) return false;
        return getAvailableProcessors() == that.getAvailableProcessors();

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(getLoadAverage());
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + getAvailableProcessors();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "loadAverage=" + loadAverage +
                ", availableProcessors=" + availableProcessors +
                '}';
    }
}
