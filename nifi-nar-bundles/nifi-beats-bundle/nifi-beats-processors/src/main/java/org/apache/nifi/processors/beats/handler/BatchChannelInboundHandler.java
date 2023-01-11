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
package org.apache.nifi.processors.beats.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.beats.protocol.Batch;
import org.apache.nifi.processors.beats.protocol.BatchMessage;
import org.apache.nifi.processors.beats.protocol.MessageAck;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Batch Channel Inbound Handler processes a batch of messages and sends an acknowledgement for the last sequence number
 */
@ChannelHandler.Sharable
public class BatchChannelInboundHandler extends SimpleChannelInboundHandler<Batch> {
    private final ComponentLog log;

    private final BlockingQueue<BatchMessage> messages;

    /**
     * Batch Channel Inbound Handler with required arguments
     *
     * @param log Processor Log
     * @param messages Queue of messages
     */
    public BatchChannelInboundHandler(final ComponentLog log, final BlockingQueue<BatchMessage> messages) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.messages = Objects.requireNonNull(messages, "Message Queue required");
    }

    /**
     * Channel Read processes a batch of messages and sends an acknowledgement for the last sequence number
     *
     * @param context Channel Handler Context
     * @param batch Batch of messages
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext context, final Batch batch) {
        Integer lastSequenceNumber = null;

        final Collection<BatchMessage> batchMessages = batch.getMessages();
        int queued = 0;
        for (final BatchMessage batchMessage : batchMessages) {
            final int sequenceNumber = batchMessage.getSequenceNumber();
            final String sender = batchMessage.getSender();
            if (messages.offer(batchMessage)) {
                log.debug("Message Sequence Number [{}] Sender [{}] queued", sequenceNumber, sender);
                lastSequenceNumber = batchMessage.getSequenceNumber();
                queued++;
            } else {
                log.warn("Message Sequence Number [{}] Sender [{}] queuing failed: Queued [{}] of [{}]", sequenceNumber, sender, queued, batchMessages.size());
                break;
            }
        }

        if (lastSequenceNumber == null) {
            log.warn("Batch Messages [{}] queuing failed", batch.getMessages().size());
        } else {
            final MessageAck messageAck = new MessageAck(lastSequenceNumber);
            context.writeAndFlush(messageAck);
        }
    }
}
