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

package org.apache.nifi.stateless.repository;

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class StatelessContentClaimWriteCache implements ContentClaimWriteCache {
    private final ContentRepository contentRepository;
    private final List<OutputStream> writtenTo = new ArrayList<>();

    public StatelessContentClaimWriteCache(final ContentRepository contentRepository) {
        this.contentRepository = contentRepository;
    }

    @Override
    public void reset() {
        for (final OutputStream stream : writtenTo) {
            try {
                stream.close();
            } catch (IOException e) {
                throw new ProcessException("Failed to close OutputStream", e);
            }
        }

        writtenTo.clear();
    }

    @Override
    public ContentClaim getContentClaim() throws IOException {
        return contentRepository.create(false);
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        final OutputStream out = contentRepository.write(claim);
        writtenTo.add(out);
        return out;
    }

    @Override
    public void flush(final ContentClaim contentClaim) {
    }

    @Override
    public void flush(final ResourceClaim claim) {
    }

    @Override
    public void flush() {
    }
}
