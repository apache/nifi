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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.event.transport;

import javax.security.auth.x500.X500Principal;

public class SslSessionStatus {
    private final X500Principal subject;
    private final X500Principal issuer;

    public SslSessionStatus(final X500Principal subject, final X500Principal issuer) {
        this.subject = subject;
        this.issuer = issuer;
    }

    public X500Principal getSubject() {
        return subject;
    }

    public X500Principal getIssuer() {
        return issuer;
    }
}
