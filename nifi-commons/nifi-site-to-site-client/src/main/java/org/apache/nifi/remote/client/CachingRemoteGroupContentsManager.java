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

package org.apache.nifi.remote.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingRemoteGroupContentsManager implements RemoteGroupContentsManager {
    private static transient final Logger logger = LoggerFactory.getLogger(CachingRemoteGroupContentsManager.class);

    private final transient ConcurrentMap<String, RemoteGroupContents> contentsMap = new ConcurrentHashMap<>();
    private final transient long refreshMillis;
    private volatile transient long lastPruneTimestamp = System.currentTimeMillis();

    public CachingRemoteGroupContentsManager(final long refreshMillis) {
        this.refreshMillis = refreshMillis;
    }


    @Override
    public ControllerDTO getRemoteContents(final String url, final SiteToSiteRestApiClient client) throws IOException {
        // Periodically prune the map so that we are not keeping entries around forever, in case an RPG is removed
        // from he canvas, etc. We want to ensure that we avoid memory leaks, even if they are likely to not cause a problem.
        if (System.currentTimeMillis() > lastPruneTimestamp + 4 * refreshMillis) {
            prune();
        }

        final String internedUrl = url.intern();
        synchronized (internedUrl) {
            final RemoteGroupContents groupContents = contentsMap.get(url);

            if (groupContents == null || groupContents.getContents() == null || groupContents.isOlderThan(refreshMillis)) {
                logger.debug("No Contents for remote group at URL {} or contents have expired; will refresh contents", url);

                final ControllerDTO refreshedContents;
                try {
                    refreshedContents = client.getController(url);
                } catch (final Exception e) {
                    // we failed to refresh contents, but we don't want to constantly poll the remote instance, failing.
                    // So we put the ControllerDTO back but use a new RemoteGroupContents so that we get a new timestamp.
                    final ControllerDTO existingController = groupContents == null ? null : groupContents.getContents();
                    final RemoteGroupContents updatedContents = new RemoteGroupContents(existingController);
                    contentsMap.put(url, updatedContents);
                    throw e;
                }

                logger.debug("Successfully retrieved contents for remote group at URL {}", url);

                final RemoteGroupContents updatedContents = new RemoteGroupContents(refreshedContents);
                contentsMap.put(url, updatedContents);
                return refreshedContents;
            }

            logger.debug("Contents for remote group at URL {} have already been fetched and have not yet expired. Will return the cached value.", url);
            return groupContents.getContents();
        }
    }

    private void prune() {
        for (final Map.Entry<String, RemoteGroupContents> entry : contentsMap.entrySet()) {
            final String url = entry.getKey();
            final RemoteGroupContents contents = entry.getValue();

            // If any entry in the map is more than 4 times as old as the refresh period,
            // then we can go ahead and remove it from the map. We use 4 * refreshMillis
            // just to ensure that we don't have any race condition with the above #getRemoteContents.
            if (contents.isOlderThan(refreshMillis * 4)) {
                contentsMap.remove(url, contents);
            }
        }
    }


    private static class RemoteGroupContents implements Serializable {
        private final ControllerDTO contents;
        private final long timestamp;

        public RemoteGroupContents(final ControllerDTO contents) {
            this.contents = contents;
            this.timestamp = System.currentTimeMillis();
        }

        public ControllerDTO getContents() {
            return contents;
        }

        public boolean isOlderThan(final long millis) {
            final long millisSinceRefresh = System.currentTimeMillis() - timestamp;
            return millisSinceRefresh > millis;
        }
    }
}
