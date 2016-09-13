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
package org.apache.nifi.remote.io.http;

import org.apache.nifi.remote.AbstractCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.remote.protocol.CommunicationsOutput;

import java.io.IOException;

public class HttpCommunicationsSession extends AbstractCommunicationsSession {

    protected int timeout = 30000;

    protected final HttpInput input;
    protected final HttpOutput output;
    protected String checksum;
    private String dataTransferUrl;

    public HttpCommunicationsSession(){
        super();
        this.input = new HttpInput();
        this.output = new HttpOutput();
    }

    @Override
    public void setTimeout(final int millis) throws IOException {
        this.timeout = millis;
    }

    @Override
    public int getTimeout() throws IOException {
        return timeout;
    }

    @Override
    public CommunicationsInput getInput() {
        return input;
    }

    @Override
    public CommunicationsOutput getOutput() {
        return output;
    }

    @Override
    public boolean isDataAvailable() {
        return false;
    }

    @Override
    public long getBytesWritten() {
        return output.getBytesWritten();
    }

    @Override
    public long getBytesRead() {
        return input.getBytesRead();
    }

    @Override
    public void interrupt() {
        input.interrupt();
        output.interrupt();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    /**
     * @param dataTransferUrl Set data transfer url to use as provenance event transit url.
     */
    public void setDataTransferUrl(String dataTransferUrl) {
        this.dataTransferUrl = dataTransferUrl;
    }

    @Override
    public String createTransitUri(String communicantUrl, String sourceFlowFileIdentifier) {
        return dataTransferUrl;
    }
}
