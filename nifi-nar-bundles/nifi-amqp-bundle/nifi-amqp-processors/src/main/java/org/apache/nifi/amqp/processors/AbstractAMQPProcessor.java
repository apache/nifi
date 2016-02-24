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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Base processor that uses RabbitMQ client API
 * (https://www.rabbitmq.com/api-guide.html) to rendezvous with AMQP-based
 * messaging systems version 0.9.1
 *
 * @param <T> the type of {@link AMQPWorker}. Please see {@link AMQPPublisher}
 *            and {@link AMQPConsumer}
 */
abstract class AbstractAMQPProcessor<T extends AMQPWorker> extends AbstractProcessor {

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("Host Name")
            .description("Network address of AMQP broker (e.g., localhost)")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Numeric value identifying Port of AMQP broker (e.g., 5671)")
            .required(true)
            .defaultValue("5672")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor V_HOST = new PropertyDescriptor.Builder()
            .name("Virtual Host")
            .description("Virtual Host name which segregates AMQP system for enhanced security.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .description("User Name used for authentication and authorization.")
            .required(true)
            .defaultValue("guest")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password used for authentication and authorization.")
            .required(true)
            .defaultValue("guest")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor AMQP_VERSION = new PropertyDescriptor.Builder()
            .name("AMQP Version")
            .description("AMQP Version. Currently only supports AMQP v0.9.1.")
            .required(true)
            .allowableValues("0.9.1")
            .defaultValue("0.9.1")
            .build();

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    static {
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(V_HOST);
        descriptors.add(USER);
        descriptors.add(PASSWORD);
        descriptors.add(AMQP_VERSION);
    }

    protected volatile Connection amqpConnection;

    protected volatile T targetResource;

    /**
     * Will builds target resource ({@link AMQPPublisher} or
     * {@link AMQPConsumer}) upon first invocation and will delegate to the
     * implementation of
     * {@link #rendezvousWithAmqp(ProcessContext, ProcessSession)} method for
     * further processing.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        synchronized (this) {
            this.buildTargetResource(context);
        }
        this.rendezvousWithAmqp(context, session);
    }

    /**
     * Will close current AMQP connection.
     */
    @OnStopped
    public void close() {
        try {
            if (this.targetResource != null) {
                this.targetResource.close();
            }
        } catch (Exception e) {
            this.getLogger().warn("Failure while closing target resource " + this.targetResource, e);
        }
        try {
            if (this.amqpConnection != null) {
                this.amqpConnection.close();
            }
        } catch (IOException e) {
            this.getLogger().warn("Failure while closing connection", e);
        }
        this.amqpConnection = null;
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)}. It is implemented by
     * sub-classes to perform {@link Processor} specific functionality.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @param session
     *            instance of {@link ProcessSession}
     */
    protected abstract void rendezvousWithAmqp(ProcessContext context, ProcessSession session) throws ProcessException;

    /**
     * Delegate method to supplement building of target {@link AMQPWorker} (see
     * {@link AMQPPublisher} or {@link AMQPConsumer}) and is implemented by
     * sub-classes.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @return new instance of {@link AMQPWorker}
     */
    protected abstract T finishBuildingTargetResource(ProcessContext context);

    /**
     * Builds target resource ({@link AMQPPublisher} or {@link AMQPConsumer}).
     * It does so by making a {@link Connection} and then delegating to the
     * implementation of {@link #finishBuildingTargetResource(ProcessContext)}
     * which will build {@link AMQPWorker} (see {@link AMQPPublisher} or
     * {@link AMQPConsumer}).
     */
    private void buildTargetResource(ProcessContext context) {
        if (this.amqpConnection == null || !this.amqpConnection.isOpen()) {
            this.amqpConnection = this.createConnection(context);
            this.targetResource = this.finishBuildingTargetResource(context);
        }
    }

    /**
     * Creates {@link Connection} to AMQP system.
     */
    private Connection createConnection(ProcessContext context) {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost(context.getProperty(HOST).getValue());
        cf.setPort(Integer.parseInt(context.getProperty(PORT).getValue()));
        cf.setUsername(context.getProperty(USER).getValue());
        cf.setPassword(context.getProperty(PASSWORD).getValue());
        String vHost = context.getProperty(V_HOST).getValue();
        if (vHost != null) {
            cf.setVirtualHost(vHost);
        }

        try {
            Connection connection = cf.newConnection();
            return connection;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to establish connection with AMQP Broker: " + cf.toString(), e);
        }
    }
}
