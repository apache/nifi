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
package org.apache.nifi.io.socket;

import javax.net.ssl.SSLContext;

public final class SocketConfiguration {

    private Integer socketTimeout;
    private Integer receiveBufferSize;
    private Integer sendBufferSize;
    private Boolean reuseAddress;
    private Boolean keepAlive;
    private Boolean oobInline;
    private Boolean tcpNoDelay;
    private Integer trafficClass;
    private SSLContext sslContext;

    public SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(final Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Boolean getReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(final Boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public Boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(final Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public Boolean getOobInline() {
        return oobInline;
    }

    public void setOobInline(final Boolean oobInline) {
        this.oobInline = oobInline;
    }

    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(final Integer receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(final Integer sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public Boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(final Boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public Integer getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(final Integer trafficClass) {
        this.trafficClass = trafficClass;
    }

}
