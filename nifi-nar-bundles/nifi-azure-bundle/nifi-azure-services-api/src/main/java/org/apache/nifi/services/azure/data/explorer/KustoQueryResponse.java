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
package org.apache.nifi.services.azure.data.explorer;

import java.io.InputStream;
import java.util.Objects;

public class KustoQueryResponse {

    private final InputStream responseStream;

    private final boolean error;

    private final String errorMessage;

    public KustoQueryResponse(final boolean error, final String errorMessage) {
        this.responseStream = null;
        this.error = error;
        this.errorMessage = errorMessage;
    }

    public KustoQueryResponse(final InputStream responseStream) {
        this.responseStream = Objects.requireNonNull(responseStream, "Response Stream required");
        this.error = false;
        this.errorMessage = null;
    }

    public InputStream getResponseStream() {
        return responseStream;
    }

    public boolean isError() {
        return error;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
