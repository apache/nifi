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


package org.apache.nifi.testharness.samples.mock;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class GetHTTPMock extends MockProcessor {

    private final String contentType;
    private final Supplier<InputStream> inputStreamSupplier;

    public GetHTTPMock(String contentType, Supplier<InputStream> inputStreamSupplier) {
        super("org.apache.nifi.processors.standard.GetHTTP");

        this.contentType = contentType;
        this.inputStreamSupplier = inputStreamSupplier;
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files are transferred to the success relationship")
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory processSessionFactory) {

        final ComponentLog logger = getLogger();

        final StopWatch stopWatch = new StopWatch(true);

        final ProcessSession session = processSessionFactory.createSession();

        final String url = context.getProperty("URL").evaluateAttributeExpressions().getValue();
        final URI uri;
        String source = url;
        try {
            uri = new URI(url);
            source = uri.getHost();
        } catch (final URISyntaxException swallow) {
            // this won't happen as the url has already been validated
        }

        FlowFile flowFile = session.create();

        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), context.getProperty("Filename").evaluateAttributeExpressions().getValue());
        flowFile = session.putAttribute(flowFile, this.getClass().getSimpleName().toLowerCase() + ".remote.source", source);
        flowFile = session.importFrom(inputStreamSupplier.get(), flowFile);

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), contentType);

        final long flowFileSize = flowFile.getSize();
        stopWatch.stop();
        session.getProvenanceReporter().receive(flowFile, url, stopWatch.getDuration(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);

        final String dataRate = stopWatch.calculateDataRate(flowFileSize);
        logger.info("Successfully received {} from {} at a rate of {}; transferred to success", new Object[]{flowFile, url, dataRate});
        session.commit();

    }
}
