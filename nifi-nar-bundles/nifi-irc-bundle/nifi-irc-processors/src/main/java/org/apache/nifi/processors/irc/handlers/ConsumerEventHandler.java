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
package org.apache.nifi.processors.irc.handlers;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Invoke;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.irc.ConsumeIRC;
import org.apache.nifi.util.StopWatch;
import org.kitteh.irc.client.library.event.channel.ChannelMessageEvent;
import org.kitteh.irc.client.library.event.helper.MessageEvent;
import org.kitteh.irc.client.library.event.user.PrivateMessageEvent;
import org.kitteh.irc.client.library.util.Format;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ConsumerEventHandler extends EventHandler {

    public ConsumerEventHandler(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger) {
        super(context, sessionFactory, logger);
    }

    @Handler(delivery = Invoke.Asynchronously)
    protected void onPrivateMessageReceived(PrivateMessageEvent event) {
        logger.info("Received private message '{}' from {} while waiting for messages on {} ",
                new Object[] {event.getMessage(), event.getActor().getName(), context.getProperty(ConsumeIRC.IRC_CHANNEL).getValue()});
        if (context.getProperty(ConsumeIRC.IRC_PROCESS_PRIV_MESSAGES).asBoolean()) {
            turnEventIntoFlowFile(event);
        } else {
            event.sendReply(String.format("Hi %s. Thank you for your message but I am not looking to chat with strangers.",
                    String.valueOf(event.getActor().getNick())));
        }
    }

    @Handler(delivery = Invoke.Asynchronously)
    protected void onChannelMessageReceived(ChannelMessageEvent event) {
        // verify if the message was sent to the channel the processor is consuming
        if (event.getChannel().getName().equals(context.getProperty(ConsumeIRC.IRC_CHANNEL).getValue())) {

            logger.info("Received message '{}'  on channel {} while waiting for messages on {} ",
                    new Object[]{event.getMessage(), event.getChannel().getName(), context.getProperty(ConsumeIRC.IRC_CHANNEL).getValue()});
            turnEventIntoFlowFile(event);
        }
    }

    private void turnEventIntoFlowFile(final MessageEvent messageEvent) {
        final ProcessSession processSession = sessionFactory.createSession();
        final StopWatch watch = new StopWatch();
        watch.start();
        try {
            FlowFile flowFile = processSession.create();


            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    if (context.getProperty(ConsumeIRC.IRC_STRIP_FORMATTING).asBoolean()) {
                        out.write(Format.stripAll(messageEvent.getMessage()).getBytes());
                    } else {
                        out.write(messageEvent.getMessage().getBytes());
                    }
                }
            });

            final Map<String, String> attributes = new HashMap<>();


            // Extract metadata and add as attributes of a channel message via casting
            if (messageEvent instanceof ChannelMessageEvent) {
                attributes.put("irc.sender", ((ChannelMessageEvent) messageEvent).getActor().getName());
                attributes.put("irc.channel", ((ChannelMessageEvent) messageEvent).getChannel().getName());
            }
            // Private messages lack channels
            if (messageEvent instanceof PrivateMessageEvent) {
                attributes.put("irc.sender", ((PrivateMessageEvent) messageEvent).getActor().getName());
            }

            // But all come from servers
            attributes.put("irc.server", messageEvent.getClient().getServerInfo().getAddress().get());
            flowFile = processSession.putAllAttributes(flowFile, attributes);

            watch.stop();
            processSession.getProvenanceReporter()
                    .receive(flowFile, "irc://"
                            .concat(messageEvent.getClient().getServerInfo().getAddress().get())
                            .concat("/")
                            // Device if append channel to URI or not
                            .concat((messageEvent instanceof ChannelMessageEvent) ? ((ChannelMessageEvent) messageEvent).getChannel().getName().concat("/") : ""),
                            watch.getDuration(TimeUnit.MILLISECONDS)
                            );

            processSession.transfer(flowFile, ConsumeIRC.REL_SUCCESS);
            processSession.commit();
        } catch (FlowFileAccessException | IllegalStateException ex) {
            logger.error("Unable to fully process input due to " + ex.getMessage(), ex);
            throw ex;
        } finally {
            processSession.rollback();
        }
    }
}

