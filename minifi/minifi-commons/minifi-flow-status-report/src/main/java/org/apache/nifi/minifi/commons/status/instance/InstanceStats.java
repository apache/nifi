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

public class InstanceStats implements java.io.Serializable {
    private long bytesRead;
    private long bytesWritten;
    private long bytesSent;
    private int flowfilesSent;
    private long bytesTransferred;
    private int flowfilesTransferred;
    private long bytesReceived;
    private int flowfilesReceived;

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(final long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(final long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(final long bytesSent) {
        this.bytesSent = bytesSent;
    }

    public int getFlowfilesSent() {
        return flowfilesSent;
    }

    public void setFlowfilesSent(final int flowfilesSent) {
        this.flowfilesSent = flowfilesSent;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(final long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    public int getFlowfilesTransferred() {
        return flowfilesTransferred;
    }

    public void setFlowfilesTransferred(final int flowfilesTransferred) {
        this.flowfilesTransferred = flowfilesTransferred;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(final long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    public int getFlowfilesReceived() {
        return flowfilesReceived;
    }

    public void setFlowfilesReceived(final int flowfilesReceived) {
        this.flowfilesReceived = flowfilesReceived;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final InstanceStats that = (InstanceStats) o;

        if (getBytesRead() != that.getBytesRead()) {
            return false;
        }
        if (getBytesWritten() != that.getBytesWritten()) {
            return false;
        }
        if (getBytesSent() != that.getBytesSent()) {
            return false;
        }
        if (getFlowfilesSent() != that.getFlowfilesSent()) {
            return false;
        }
        if (getBytesTransferred() != that.getBytesTransferred()) {
            return false;
        }
        if (getFlowfilesTransferred() != that.getFlowfilesTransferred()) {
            return false;
        }
        if (getBytesReceived() != that.getBytesReceived()) {
            return false;
        }
        return getFlowfilesReceived() == that.getFlowfilesReceived();

    }

    @Override
    public int hashCode() {
        int result = (int) (getBytesRead() ^ (getBytesRead() >>> 32));
        result = 31 * result + (int) (getBytesWritten() ^ (getBytesWritten() >>> 32));
        result = 31 * result + (int) (getBytesSent() ^ (getBytesSent() >>> 32));
        result = 31 * result + getFlowfilesSent();
        result = 31 * result + (int) (getBytesTransferred() ^ (getBytesTransferred() >>> 32));
        result = 31 * result + getFlowfilesTransferred();
        result = 31 * result + (int) (getBytesReceived() ^ (getBytesReceived() >>> 32));
        result = 31 * result + getFlowfilesReceived();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "bytesRead=" + bytesRead +
                ", bytesWritten=" + bytesWritten +
                ", bytesSent=" + bytesSent +
                ", flowfilesSent=" + flowfilesSent +
                ", bytesTransferred=" + bytesTransferred +
                ", flowfilesTransferred=" + flowfilesTransferred +
                ", bytesReceived=" + bytesReceived +
                ", flowfilesReceived=" + flowfilesReceived +
                '}';
    }
}
