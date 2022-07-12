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
package org.apache.nifi.processors.smb;

import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;

public class NiFiSmbClientFactory {

    NiFiSmbClient create(Session session, String shareName) {
        final Share share = session.connectShare(shareName);
        if (share instanceof DiskShare) {
            return new NiFiSmbClient(session, (DiskShare) share);
        } else {
            throw new IllegalArgumentException("NiFi supports only disk shares but " +
                    share.getClass().getSimpleName() + " found on host " + session.getConnection().getRemoteHostname()
                    + "!");
        }
    }

}
