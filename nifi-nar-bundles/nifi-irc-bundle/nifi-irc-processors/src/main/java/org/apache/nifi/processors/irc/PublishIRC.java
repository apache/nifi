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
package org.apache.nifi.processors.irc;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.irc.handlers.PublisherEventHandler;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

@Tags({"publish", "irc"})
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)

@CapabilityDescription("This processor implements a IRC client allowing nifi to publish flowfile contents into a " +
        "predefined IRC channel. " + "\n" +
        "IMPORTANT NOTE: Due to the nature of the IRC protocol, no delivery guarantees are offered. USE WITH CARE")

@WritesAttributes({
        @WritesAttribute(attribute = "irc.sender", description = "The IRC nickname source user of the message"),
        @WritesAttribute(attribute = "irc.channel", description = "The channel from where the message was received "),
        @WritesAttribute(attribute = "irc.server", description = "The values IRC channel where the message was received from")})
public class PublishIRC extends AbstractIRCProcessor {

    private volatile PublisherEventHandler eventHandler;

    @OnStopped
    public void onUnscheduled(ProcessContext context) {
        clearSetup(client, eventHandler);
        client = null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();



        if (client == null) {
            // Initialise the handler that will be provided to the setupClient super method
            this.eventHandler = new PublisherEventHandler(context, sessionFactory, getLogger());
            this.client = ircClientService.getClient();
            this.client.getEventManager().registerEventListener(this.eventHandler);
        }

        if (ircClientService.getIsConnected().get()) {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                return;
            }

            final StopWatch watch = new StopWatch();
            watch.start();

            // ensure queue is set to no delay
            ircClientService.setAndGetDelay(0);

            final String targetChannel = context.getProperty(IRC_CHANNEL).getValue();
            // Verify if should join the channel
            if (!ircClientService.getClient().getChannels().contains(targetChannel)) {
                ircClientService.joinChannel(targetChannel);
            }


            // Do the FlowFile magic
            if (client.getChannel(targetChannel).isPresent()) {
                client.sendMessage(targetChannel, readContent(session, flowFile).toString());
                session.transfer(flowFile, REL_SUCCESS);
                watch.stop();
                session.getProvenanceReporter().send(flowFile, "irc://"
                                .concat(client.getServerInfo().getAddress().get())
                                .concat("/")
                                // Device if append channel to URI or not
                                .concat(targetChannel.concat("/")),
                        watch.getDuration(TimeUnit.MILLISECONDS)
                );
            } else {
                // The client seems to be waiting for join command to complete, rollback for now
                session.rollback(false);
            }
            // Commit no matter what
            session.commit();
        }
        context.yield();
    }

    /**
     * Helper method to read the FlowFile content stream into a ByteArrayOutputStream object.
     *
     * @param session
     *            - the current process session.
     * @param flowFile
     *            - the FlowFile to read the content from.
     *
     * @return ByteArrayOutputStream object containing the FlowFile content.
     */
    protected ByteArrayOutputStream readContent(final ProcessSession session, final FlowFile flowFile) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize() + 1);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.copy(in, baos);
            }
        });

        return baos;
    }

}
