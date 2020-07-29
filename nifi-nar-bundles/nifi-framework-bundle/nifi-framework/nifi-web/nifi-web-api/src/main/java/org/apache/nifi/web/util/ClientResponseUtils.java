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
package org.apache.nifi.web.util;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.stream.io.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class ClientResponseUtils {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(ClientResponseUtils.class));

    public static void drainClientResponse(final Response response) {
        if (response != null) {
            BufferedInputStream bis = null;
            try {
                bis = new BufferedInputStream(response.readEntity(InputStream.class));
                IOUtils.copy(bis, new NullOutputStream());
            } catch (final IOException ioe) {
                logger.info("Failed clearing out non-client response buffer due to: " + ioe, ioe);
            } finally {
                IOUtils.closeQuietly(bis);
            }
        }
    }
}
