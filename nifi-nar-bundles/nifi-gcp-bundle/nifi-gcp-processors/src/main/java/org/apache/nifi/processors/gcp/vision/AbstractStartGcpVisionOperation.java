/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.gcp.vision;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public abstract class AbstractStartGcpVisionOperation<B extends com.google.protobuf.GeneratedMessageV3.Builder<B>> extends AbstractGcpVisionProcessor  {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null && !context.getProperty(getJsonPayloadPropertyDescriptor()).isSet()) {
            return;
        } else if (flowFile == null) {
            flowFile = session.create();
        }
        try  {
            OperationFuture<?, ?> asyncResponse = startOperation(session, context, flowFile);
            String operationName = asyncResponse.getName();
            session.putAttribute(flowFile, GCP_OPERATION_KEY, operationName);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Fail to start GCP Vision operation", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @OnStopped
    public void onStopped() throws IOException {
        getVisionClient().close();
    }

    protected OperationFuture<?, ?> startOperation(ProcessSession session, ProcessContext context, FlowFile flowFile) {
        B builder = newBuilder();
        InputStream inStream = context.getProperty(getJsonPayloadPropertyDescriptor()).isSet()
                ? getInputStreamFromProperty(context, flowFile) : session.read(flowFile);
        try (InputStream inputStream = inStream) {
            JsonFormat.parser().ignoringUnknownFields().merge(new InputStreamReader(inputStream), builder);
        } catch (final IOException e) {
            throw new ProcessException("Read FlowFile Failed", e);
        }
        return startOperation(builder);
    }

    private InputStream getInputStreamFromProperty(ProcessContext context, FlowFile flowFile) {
        return new ByteArrayInputStream(context.getProperty(getJsonPayloadPropertyDescriptor()).evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8));
    }

    abstract B newBuilder();

    abstract OperationFuture<?, ?> startOperation(B builder);

    abstract PropertyDescriptor getJsonPayloadPropertyDescriptor();
}
