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

package org.apache.nifi.processors.websocket;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractWebSocketProcessor extends AbstractSessionFactoryProcessor {

    public static final String ATTR_WS_CS_ID = "websocket.controller.service.id";
    public static final String ATTR_WS_SESSION_ID = "websocket.session.id";
    public static final String ATTR_WS_ENDPOINT_ID = "websocket.endpoint.id";
    public static final String ATTR_WS_FAILURE_DETAIL = "websocket.failure.detail";
    public static final String ATTR_WS_MESSAGE_TYPE = "websocket.message.type";
    public static final String ATTR_WS_LOCAL_ADDRESS = "websocket.local.address";
    public static final String ATTR_WS_REMOTE_ADDRESS = "websocket.remote.address";

    static List<PropertyDescriptor> getAbstractPropertyDescriptors(){
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        return descriptors;
    }

    protected ComponentLog logger;
    protected ProcessSessionFactory processSessionFactory;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    public interface WebSocketFunction {
        void execute(final WebSocketService webSocketService) throws IOException, WebSocketConfigurationException;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (processSessionFactory == null) {
            processSessionFactory = sessionFactory;
        }
        ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commit();
        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw t;
        }
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

}
