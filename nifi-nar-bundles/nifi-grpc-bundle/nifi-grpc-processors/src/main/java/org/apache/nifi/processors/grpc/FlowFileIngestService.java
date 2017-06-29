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
package org.apache.nifi.processors.grpc;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple gRPC service that handles receipt of FlowFileRequest messages from external-to-NiFi gRPC
 * clients.
 */
// NOTE: you may need to add the sources generated after running `maven clean compile` to your IDE
// configured source directories. Otherwise, the classes generated when the proto is compiled won't
// be accessible from here. For IntelliJ, open this module's settings and mark the following as source directories:
//
// * target/generated-sources/protobuf/grpc-java
// * target/generated-sources/protobuf/java
public class FlowFileIngestService extends FlowFileServiceGrpc.FlowFileServiceImplBase {
    public static final String SERVICE_NAME = "grpc://FlowFileIngestService";
    public static final int FILES_BEFORE_CHECKING_DESTINATION_SPACE = 5;

    private final AtomicLong filesReceived = new AtomicLong(0L);
    private final AtomicBoolean spaceAvailable = new AtomicBoolean(true);
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference;
    private final ProcessContext context;
    private final ComponentLog logger;

    /**
     * Create a FlowFileIngestService
     *
     * @param sessionFactoryReference a reference to a {@link ProcessSessionFactory} to route {@link
     *                                FlowFile}s to process relationships.
     */
    public FlowFileIngestService(final ComponentLog logger,
                                 final AtomicReference<ProcessSessionFactory> sessionFactoryReference,
                                 final ProcessContext context) {
        this.context = checkNotNull(context);
        this.sessionFactoryReference = checkNotNull(sessionFactoryReference);
        this.logger = checkNotNull(logger);
    }

    /**
     * Handle receipt of a FlowFileRequest and route it to the appropriate process relationship.
     *
     * @param request          the flowfile request
     * @param responseObserver the mechanism by which to reply to the client
     */
    @Override
    public void send(final org.apache.nifi.processors.grpc.FlowFileRequest request, final StreamObserver<FlowFileReply> responseObserver) {
        final FlowFileReply.Builder replyBuilder = FlowFileReply.newBuilder();

        final String remoteHost = FlowFileIngestServiceInterceptor.REMOTE_HOST_KEY.get();
        final String remoteDN = FlowFileIngestServiceInterceptor.REMOTE_DN_KEY.get();

        // block until we have a session factory (occurs when processor is triggered)
        ProcessSessionFactory sessionFactory = null;
        while (sessionFactory == null) {
            sessionFactory = sessionFactoryReference.get();
            if (sessionFactory == null) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                }
            }
        }

        final ProcessSession session = sessionFactory.createSession();

        // if there's no space available, reject the request.
        final long n = filesReceived.getAndIncrement() % FILES_BEFORE_CHECKING_DESTINATION_SPACE;
        if (n == 0 || !spaceAvailable.get()) {
            if (context.getAvailableRelationships().isEmpty()) {
                spaceAvailable.set(false);
                final String message = "Received request from " + remoteHost + " but no space available; Indicating Service Unavailable";
                if (logger.isDebugEnabled()) {
                    logger.debug(message);
                }
                final FlowFileReply reply = replyBuilder.setResponseCode(FlowFileReply.ResponseCode.ERROR)
                        .setBody(message)
                        .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            } else {
                spaceAvailable.set(true);
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Received request from " + remoteHost);
        }

        final long startNanos = System.nanoTime();
        FlowFile flowFile = session.create();
        // push the attributes provided onto the created flowfile
        final Map<String, String> attributes = Maps.newHashMap();
        attributes.putAll(request.getAttributesMap());
        String sourceSystemFlowFileIdentifier = attributes.get(CoreAttributes.UUID.key());
        if (sourceSystemFlowFileIdentifier != null) {
            sourceSystemFlowFileIdentifier = "urn:nifi:" + sourceSystemFlowFileIdentifier;

            // If we receveied a UUID, we want to give the FlowFile a new UUID and register the sending system's
            // identifier as the SourceSystemFlowFileIdentifier field in the Provenance RECEIVE event
            attributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        final ByteString content = request.getContent();
        final InputStream contentStream = content.newInput();

        // write the provided content to the flowfile
        flowFile = session.write(flowFile, out -> {
            try (final BufferedOutputStream bos = new BufferedOutputStream(out, 65536)) {
                IOUtils.copy(contentStream, bos);
            }
        });
        final long transferNanos = System.nanoTime() - startNanos;
        final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);

        session.getProvenanceReporter().receive(flowFile,
                SERVICE_NAME,
                sourceSystemFlowFileIdentifier,
                "Remote DN=" + remoteDN,
                transferMillis);
        flowFile = session.putAttribute(flowFile, ListenGRPC.REMOTE_HOST, remoteHost);
        flowFile = session.putAttribute(flowFile, ListenGRPC.REMOTE_USER_DN, remoteDN);

        // register success
        session.transfer(flowFile, ListenGRPC.REL_SUCCESS);
        session.commit();

        // reply to client
        final FlowFileReply reply = replyBuilder.setResponseCode(FlowFileReply.ResponseCode.SUCCESS)
                .setBody("FlowFile successfully received.")
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
