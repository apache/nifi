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

public class ConnectionStats implements java.io.Serializable {
    private int inputCount;
    private long inputBytes;
    private int outputCount;
    private long outputBytes;

    public ConnectionStats() {
    }

    public int getInputCount() {
        return inputCount;
    }

    public void setInputCount(int inputCount) {
        this.inputCount = inputCount;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public int getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(int outputCount) {
        this.outputCount = outputCount;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionStats that = (ConnectionStats) o;

        if (getInputCount() != that.getInputCount()) return false;
        if (getInputBytes() != that.getInputBytes()) return false;
        if (getOutputCount() != that.getOutputCount()) return false;
        return getOutputBytes() == that.getOutputBytes();

    }

    @Override
    public int hashCode() {
        int result = getInputCount();
        result = 31 * result + (int) (getInputBytes() ^ (getInputBytes() >>> 32));
        result = 31 * result + getOutputCount();
        result = 31 * result + (int) (getOutputBytes() ^ (getOutputBytes() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "inputCount=" + inputCount +
                ", inputBytes=" + inputBytes +
                ", outputCount=" + outputCount +
                ", outputBytes=" + outputBytes +
                '}';
    }
}
