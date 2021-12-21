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
package org.apache.nifi.processors.standard.ssh;

import net.schmizz.sshj.common.SSHException;
import net.schmizz.sshj.connection.channel.direct.SessionFactory;
import net.schmizz.sshj.sftp.PacketType;
import net.schmizz.sshj.sftp.RenameFlags;
import net.schmizz.sshj.sftp.Request;
import net.schmizz.sshj.sftp.SFTPEngine;
import net.schmizz.sshj.sftp.SFTPException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Patched SFTP Engine to workaround SFTP rename issue 751 in SSHJ 0.32.0
 *
 * This class can be removed once the issue is resolved in a future version of SSHJ
 */
public class PatchedSFTPEngine extends SFTPEngine {

    public PatchedSFTPEngine(final SessionFactory sessionFactory) throws SSHException {
        super(sessionFactory);
    }

    /**
     * Override rename request packet generation to workaround handling of rename flags
     *
     * @param oldPath Old path of file to be renamed
     * @param newPath New path of file to be renamed
     * @param flags Rename flags used for SFTP Version 5 or higher
     * @throws IOException Thrown on unsupported protocol version or request processing failures
     */
    @Override
    public void rename(final String oldPath, final String newPath, final Set<RenameFlags> flags) throws IOException {
        if (operativeVersion < 1) {
            throw new SFTPException("RENAME is not supported in SFTPv" + operativeVersion);
        }

        final Charset remoteCharset = sub.getRemoteCharset();
        final Request request = newRequest(PacketType.RENAME)
                .putString(oldPath, remoteCharset)
                .putString(newPath, remoteCharset);
        // SFTP Version 5 introduced rename flags according to Section 6.5 of the specification
        if (operativeVersion >= 5) {
            long renameFlagMask = 0L;
            for (RenameFlags flag : flags) {
                renameFlagMask = renameFlagMask | flag.longValue();
            }
            request.putUInt32(renameFlagMask);
        }

        request(request).retrieve(getTimeoutMs(), TimeUnit.MILLISECONDS).ensureStatusPacketIsOK();
    }
}
