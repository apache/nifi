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
import com.google.cloud.vision.v1.AsyncBatchAnnotateFilesRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

@Tags({"Google", "Cloud", "Machine Learning", "Vision"})
@CapabilityDescription("Trigger a Vision operation on file input. It should be followed by GetGcpVisionAnnotateFilesOperationStatus processor in order to monitor operation status.")
@SeeAlso({GetGcpVisionAnnotateFilesOperationStatus.class})
@WritesAttributes({
        @WritesAttribute(attribute = "operationKey", description = "Unique key of the operation.")
})
public class StartGcpVisionAnnotateFilesOperation extends AbstractStartGcpVisionOperation {

    protected AsyncBatchAnnotateFilesRequest fromJson(String json) throws InvalidProtocolBufferException {
        AsyncBatchAnnotateFilesRequest.Builder builder = AsyncBatchAnnotateFilesRequest.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        return builder.build();
    }

    @Override
    protected OperationFuture startOperation(ProcessSession session, FlowFile flowFile) throws InvalidProtocolBufferException {
        AsyncBatchAnnotateFilesRequest requests = fromJson(readFlowFile(session, flowFile));
        return getVisionClient().asyncBatchAnnotateFilesAsync(requests);
    }
}
