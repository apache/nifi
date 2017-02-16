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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.irc.ConsumeIRC;
import org.kitteh.irc.client.library.event.channel.RequestedChannelJoinCompleteEvent;

// Class to generalize the Consumer and Publisher event Handlers
public abstract class EventHandler {
    protected final ProcessSessionFactory sessionFactory;
    protected final ComponentLog logger;
    protected final ProcessContext context;

    public EventHandler(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger) {
        this.sessionFactory = sessionFactory;
        this.logger = logger;
        this.context = context;
    }

    // While the join logic happens at the client service, the "onJoin" handlers are managed by the processors.
    @Handler(delivery = Invoke.Asynchronously)
    protected void onJoinComplete(RequestedChannelJoinCompleteEvent event) {
        if (event.getChannel().getName().equals(context.getProperty(ConsumeIRC.IRC_CHANNEL).getValue())) {
            logger.info("Joined channel {} ", new Object[] {event.getAffectedChannel().get().getName()});
        }
    }
}
