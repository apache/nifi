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
package org.apache.nifi.processors.flume;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.source.DefaultSourceFactory;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.flume.util.FlowFileEvent;

/**
 * This is a base class that is helpful when building processors interacting with Flume.
 */
public abstract class AbstractFlumeProcessor extends AbstractSessionFactoryProcessor {

    protected static final SourceFactory SOURCE_FACTORY = new DefaultSourceFactory();
    protected static final SinkFactory SINK_FACTORY = new DefaultSinkFactory();

    protected static Event flowFileToEvent(FlowFile flowFile, ProcessSession session) {
        return new FlowFileEvent(flowFile, session);
    }

    protected static void transferEvent(final Event event, ProcessSession session,
        Relationship relationship) {
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, event.getHeaders());

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(event.getBody());
            }
        });

        session.getProvenanceReporter()
            .create(flowFile);
        session.transfer(flowFile, relationship);
    }

    protected static Validator createSourceValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                String reason = null;
                try {
                    FlumeSourceProcessor.SOURCE_FACTORY.create("NiFi Source", value);
                } catch (Exception ex) {
                    reason = ex.getLocalizedMessage();
                    reason = Character.toLowerCase(reason.charAt(0)) + reason.substring(1);
                }
                return new ValidationResult.Builder().subject(subject)
                    .input(value)
                    .explanation(reason)
                    .valid(reason == null)
                    .build();
            }
        };
    }

    protected static Validator createSinkValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                String reason = null;
                try {
                    FlumeSinkProcessor.SINK_FACTORY.create("NiFi Sink", value);
                } catch (Exception ex) {
                    reason = ex.getLocalizedMessage();
                    reason = Character.toLowerCase(reason.charAt(0)) + reason.substring(1);
                }
                return new ValidationResult.Builder().subject(subject)
                    .input(value)
                    .explanation(reason)
                    .valid(reason == null)
                    .build();
            }
        };
    }

    protected static Context getFlumeContext(String flumeConfig, String prefix) {
        Properties flumeProperties = new Properties();
        if (flumeConfig != null) {
            try {
                flumeProperties.load(new StringReader(flumeConfig));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        Map<String, String> parameters = Maps.newHashMap();
        for (String property : flumeProperties.stringPropertyNames()) {
            parameters.put(property, flumeProperties.getProperty(property));
        }
        return new Context(new Context(parameters).getSubProperties(prefix));
    }

    protected static Context getFlumeSourceContext(String flumeConfig,
        String agentName, String sourceName) {
        return getFlumeContext(flumeConfig, agentName + ".sources." + sourceName + ".");
    }

    protected static Context getFlumeSinkContext(String flumeConfig,
        String agentName, String sinkName) {
        return getFlumeContext(flumeConfig, agentName + ".sinks." + sinkName + ".");
    }

    /*
     * Borrowed from AbstractProcessor. The FlumeSourceProcessor needs to implement this directly
     * to handle event driven sources, but it's marked final in AbstractProcessor.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commit();
        } catch (final Throwable t) {
            getLogger()
                .error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw t;
        }
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

}
