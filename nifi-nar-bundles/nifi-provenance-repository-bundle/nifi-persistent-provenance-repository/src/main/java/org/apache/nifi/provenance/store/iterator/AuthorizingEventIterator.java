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

package org.apache.nifi.provenance.store.iterator;

import java.io.IOException;
import java.util.Optional;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;

public class AuthorizingEventIterator implements EventIterator {
    private final EventIterator iterator;
    private final EventAuthorizer authorizer;
    private final EventTransformer transformer;

    public AuthorizingEventIterator(final EventIterator iterator, final EventAuthorizer authorizer,
        final EventTransformer unauthorizedTransformer) {
        this.iterator = iterator;
        this.authorizer = authorizer;
        this.transformer = unauthorizedTransformer;
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }

    @Override
    public Optional<ProvenanceEventRecord> nextEvent() throws IOException {
        while (true) {
            final Optional<ProvenanceEventRecord> next = iterator.nextEvent();
            if (!next.isPresent()) {
                return next;
            }

            if (authorizer.isAuthorized(next.get())) {
                return next;
            }

            final Optional<ProvenanceEventRecord> eventOption = transformer.transform(next.get());
            if (eventOption.isPresent()) {
                return eventOption;
            }
        }
    }

}
