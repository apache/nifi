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
package org.apache.nifi.amqp.processors;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Connection} to be used for testing. Will return the
 * same instance of {@link Channel} when {@link #createChannel()} is called.
 *
 * This class essentially emulates AMQP system and attempts to ensure the same
 * behavior on publish/subscribe and other core operations used by the NIFI AMQP
 * component.
 *
 * NOTE: Only methods that are used by the framework are implemented. More
 * could/should be added later
 */
class TestConnection implements Connection {

    private final TestChannel channel;
    private boolean open;
    private String id;

    public TestConnection(final Map<String, String> exchangeToRoutingKeyMappings, final Map<String, List<String>> routingKeyToQueueMappings) {
        this.channel = new TestChannel(exchangeToRoutingKeyMappings, routingKeyToQueueMappings);
        this.channel.setConnection(this);
        this.open = true;
    }

    @Override
    public void addShutdownListener(final ShutdownListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void removeShutdownListener(final ShutdownListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public ShutdownSignalException getCloseReason() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void notifyListeners() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public InetAddress getAddress() {
        try {
            return InetAddress.getByName("localhost");
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int getPort() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public int getChannelMax() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public int getFrameMax() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public int getHeartbeat() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public Map<String, Object> getClientProperties() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public Map<String, Object> getServerProperties() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public Channel createChannel() throws IOException {
        return this.channel;
    }

    @Override
    public Channel createChannel(final int channelNumber) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void close() throws IOException {
        this.open = false;
        try {
            this.channel.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(final int closeCode, final String closeMessage) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void close(final int timeout) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void close(final int closeCode, final String closeMessage, final int timeout) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void abort(final int closeCode, final String closeMessage) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void abort(final int timeout) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void abort(final int closeCode, final String closeMessage, final int timeout) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void addBlockedListener(final BlockedListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public boolean removeBlockedListener(final BlockedListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void clearBlockedListeners() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String getClientProvidedName() {
        return "unit-test";
    }

    @Override
    public BlockedListener addBlockedListener(final BlockedCallback blockedCallback, final UnblockedCallback unblockedCallback) {
        return null;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(final String id) {
        this.id = id;
    }
}
