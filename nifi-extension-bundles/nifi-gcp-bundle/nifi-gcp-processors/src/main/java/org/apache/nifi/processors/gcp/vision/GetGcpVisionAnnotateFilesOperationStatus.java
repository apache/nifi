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

import com.google.cloud.vision.v1p2beta1.AsyncBatchAnnotateFilesResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"Google", "Cloud", "Vision", "Machine Learning"})
@CapabilityDescription("Retrieves the current status of an Google Vision operation.")
@SeeAlso({StartGcpVisionAnnotateFilesOperation.class})
@ReadsAttributes({
        @ReadsAttribute(attribute = "operationKey", description = "A unique identifier of the operation designated by the Vision server.")
})
public class GetGcpVisionAnnotateFilesOperationStatus extends AbstractGetGcpVisionAnnotateOperationStatus {
    @Override
    protected GeneratedMessageV3 deserializeResponse(ByteString responseValue) throws InvalidProtocolBufferException {
        return AsyncBatchAnnotateFilesResponse.parseFrom(responseValue);
    }
}
