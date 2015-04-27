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
package org.apache.nifi.processors.standard.util;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class SFTPConnection implements Closeable {

    private static final Log logger = LogFactory.getLog(SFTPConnection.class);
    private final Session session;
    private final ChannelSftp sftp;

    public SFTPConnection(final Session session, final ChannelSftp sftp) {
        this.session = session;
        this.sftp = sftp;
    }

    public Session getSession() {
        return session;
    }

    public ChannelSftp getSftp() {
        return sftp;
    }

    @Override
    public void close() throws IOException {
        if (null == sftp) {
            return;
        }
        try {
            if (null != session) {
                session.disconnect();
            }
        } catch (final Exception ex) {
            /*IGNORE*/
            logger.warn("Unable to disconnect session due to " + ex);
            if (logger.isDebugEnabled()) {
                logger.warn("", ex);
            }
        }
        try {
            if (null != sftp) {
                sftp.exit();
            }
        } catch (final Exception ex) {
            /*IGNORE*/
            logger.warn("Unable to disconnect session due to " + ex);
            if (logger.isDebugEnabled()) {
                logger.warn("", ex);
            }
        }
    }
}
