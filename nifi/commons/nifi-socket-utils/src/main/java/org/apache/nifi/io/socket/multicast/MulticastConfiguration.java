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
package org.apache.nifi.io.socket.multicast;

/**
 * @author unattributed
 */
public final class MulticastConfiguration {

    private MulticastTimeToLive ttl = DEFAULT_MULTICAST_TTL;

    private Integer socketTimeout;

    private Integer receiveBufferSize;

    private Integer sendBufferSize;

    private Boolean reuseAddress;

    private Integer trafficClass;

    private Boolean loopbackMode;

    public static final MulticastTimeToLive DEFAULT_MULTICAST_TTL = MulticastTimeToLive.SAME_SUBNET;

    public MulticastTimeToLive getTtl() {
        return ttl;
    }

    public void setTtl(final MulticastTimeToLive ttl) {
        if (ttl == null) {
            throw new NullPointerException("Multicast TTL may not be null.");
        }
        this.ttl = ttl;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Boolean getReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(Boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(Integer receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(Integer sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public Integer getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(Integer trafficClass) {
        this.trafficClass = trafficClass;
    }

    public Boolean getLoopbackMode() {
        return loopbackMode;
    }

    public void setLoopbackMode(Boolean loopbackMode) {
        this.loopbackMode = loopbackMode;
    }

}
