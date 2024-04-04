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

package org.apache.nifi.controller.repository.io;

import org.apache.nifi.controller.repository.claim.ContentClaim;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ContentClaimOutputStream extends OutputStream {

    /**
     * Creates a new Content Claim that is backed by this OutputStream. This allows the caller to
     * create a new Content Claim but ensure that they keep writing to the same OutputStream, which can
     * significantly improve performance.
     *
     * @return a new ContentClaim
     * @throws IOException if unable to finalize the current ContentClaim or create a new one
     */
    abstract public ContentClaim newContentClaim() throws IOException;

}
