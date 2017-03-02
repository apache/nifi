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
package org.apache.nifi.spring.bootstrap;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.nifi.spring.SpringDataExchanger;
import org.apache.nifi.spring.SpringNiFiConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Scopes instance of itself to a dedicated {@link ClassLoader}, thus allowing
 * Spring Application Context and its class path to be modified and refreshed by
 * simply re-starting SpringContextProcessor. Also ensures that there are no
 * class path collisions between multiple instances of Spring Context Processor
 * which are loaded by the same NAR Class Loader.
 */
/*
 * This class is for internal use only and must never be instantiated by the NAR
 * Class Loader (hence in a isolated package with nothing referencing it). It is
 * loaded by a dedicated CL via byte array that represents it ensuring that this
 * class can be loaded multiple times by multiple Class Loaders within a single
 * instance of NAR.
 */
final class SpringContextDelegate implements Closeable, SpringDataExchanger {

    private final Logger logger = LoggerFactory.getLogger(SpringContextDelegate.class);

    private final ClassPathXmlApplicationContext applicationContext;

    private final MessageChannel toSpringChannel;

    private final PollableChannel fromSpringChannel;

    private final String configName;

    /**
     *
     */
    private SpringContextDelegate(String configName) {
        this.configName = configName;
        ClassLoader orig = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        if (logger.isDebugEnabled()) {
            logger.debug("Using " + Thread.currentThread().getContextClassLoader()
                    + " as context class loader while loading Spring Context '" + configName + "'.");
        }
        try {
            this.applicationContext = new ClassPathXmlApplicationContext(configName);
            if (this.applicationContext.containsBean(SpringNiFiConstants.FROM_NIFI)){
                this.toSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.FROM_NIFI, MessageChannel.class);
                if (logger.isDebugEnabled()) {
                    logger.debug("Spring Application Context defined in '" + configName
                            + "' is capable of receiving messages from NiFi since 'fromNiFi' channel was discovered.");
                }
            } else {
                this.toSpringChannel = null;
            }
            if (this.applicationContext.containsBean(SpringNiFiConstants.TO_NIFI)){
                this.fromSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.TO_NIFI, PollableChannel.class);
                if (logger.isDebugEnabled()) {
                    logger.debug("Spring Application Context defined in '" + configName
                            + "' is capable of sending messages to " + "NiFi since 'toNiFi' channel was discovered.");
                }
            } else {
                this.fromSpringChannel = null;
            }
            if (logger.isInfoEnabled() && this.toSpringChannel == null && this.fromSpringChannel == null){
                logger.info("Spring Application Context is headless since neither 'fromNiFi' nor 'toNiFi' channels were defined. "
                        + "No data will be exchanged.");
            }
        } finally {
            Thread.currentThread().setContextClassLoader(orig);
        }
    }

    /**
     *
     */
    @Override
    public <T> boolean send(T payload, Map<String, ?> messageHeaders, long timeout) {
        if (this.toSpringChannel != null){
            return this.toSpringChannel.send(MessageBuilder.withPayload(payload).copyHeaders(messageHeaders).build(), timeout);
        } else {
            throw new IllegalStateException("Failed to send message to '" + this.configName
                    + "'. There are no 'fromNiFi' channels configured which means the Application Conetxt is not set up to receive messages from NiFi");
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> SpringResponse<T> receive(long timeout) {
        if (this.fromSpringChannel != null) {
            final Message<T> message = (Message<T>) this.fromSpringChannel.receive(timeout);
            if (message != null) {
                if (!(message.getPayload() instanceof byte[]) && !(message.getPayload() instanceof String)) {
                    throw new IllegalStateException("Failed while receiving message from Spring due to the "
                            + "payload type being other then byte[] or String which are the only types that are supported. Please "
                            + "apply transformation/conversion on Spring side when sending message back to NiFi");
                }
                return new SpringResponse<T>(message.getPayload(), message.getHeaders());
            }
        }
        return null;
    }

    /**
     *
     */
    @Override
    public void close() throws IOException {
        logger.info("Closing Spring Application Context");
        this.applicationContext.close();
        if (logger.isInfoEnabled()) {
            logger.info("Closing " + this.getClass().getClassLoader());
        }
        ((URLClassLoader) this.getClass().getClassLoader()).close();
        logger.info("Successfully closed Spring Application Context and its ClassLoader.");
    }
}
