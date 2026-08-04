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
package org.apache.nifi.web.server.handler;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import java.util.List;

/**
 * Handler that rejects requests declaring an unsupported Content-Encoding
 */
public class UnsupportedContentEncodingHandler extends Handler.Abstract {

    private static final String IDENTITY_ENCODING = "identity";

    private static final String UNSUPPORTED_MESSAGE = "Content-Encoding not supported";

    @Override
    public boolean handle(final Request request, final Response response, final Callback callback) {
        // Handle one or more Content-Encoding request headers
        final List<String> contentEncodings = request.getHeaders().getValuesList(HttpHeader.CONTENT_ENCODING);

        final boolean handled;
        if (isContentEncodingUnsupported(contentEncodings)) {
            Response.writeError(request, response, callback, HttpStatus.UNSUPPORTED_MEDIA_TYPE_415, UNSUPPORTED_MESSAGE);
            handled = true;
        } else {
            handled = false;
        }

        return handled;
    }

    private boolean isContentEncodingUnsupported(final List<String> contentEncodings) {
        // Empty Content-Encoding header is allowed
        boolean unsupported = false;

        for (final String contentEncoding : contentEncodings) {
            // Content-Encoding with a value of identity is allowed indicating no encoding
            if (!IDENTITY_ENCODING.equalsIgnoreCase(contentEncoding)) {
                unsupported = true;
                break;
            }
        }

        return unsupported;
    }
}
