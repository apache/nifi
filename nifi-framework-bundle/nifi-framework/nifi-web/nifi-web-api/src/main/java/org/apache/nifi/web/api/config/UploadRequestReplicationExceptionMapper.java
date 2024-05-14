/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.web.api.config;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.nifi.cluster.coordination.http.replication.UploadRequestReplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class UploadRequestReplicationExceptionMapper implements ExceptionMapper<UploadRequestReplicationException> {

    private static final Logger logger = LoggerFactory.getLogger(UploadRequestReplicationExceptionMapper.class);

    @Override
    public Response toResponse(final UploadRequestReplicationException exception) {
        final int statusCode = exception.getStatusCode();
        logger.warn("{}. Returning {} response.", exception, statusCode, exception);
        return Response.status(statusCode).entity(exception.getMessage()).type("text/plain").build();
    }
}
