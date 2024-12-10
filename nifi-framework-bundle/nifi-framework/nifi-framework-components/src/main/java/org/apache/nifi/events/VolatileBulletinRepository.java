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
package org.apache.nifi.events;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.RingBuffer.Filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class VolatileBulletinRepository implements BulletinRepository {

    private static final int CONTROLLER_BUFFER_SIZE = 10;
    private static final int COMPONENT_BUFFER_SIZE = 5;
    private static final String CONTROLLER_BULLETIN_STORE_KEY = "CONTROLLER";
    private static final String SERVICE_BULLETIN_STORE_KEY = "SERVICE";
    private static final String REPORTING_TASK_BULLETIN_STORE_KEY = "REPORTING_TASK";
    private static final String FLOW_ANALYSIS_RULE_BULLETIN_STORE_KEY = "FLOW_ANALYSIS_RULE";
    private static final String FLOW_REGISTRY_CLIENT_STORE_KEY = "FLOW_REGISTRY_CLIENT";
    private static final String PARAMETER_PROVIDER_BULLETIN_STORE_KEY = "PARAMETER_PROVIDER";

    private final ConcurrentMap<String, ConcurrentMap<String, RingBuffer<Bulletin>>> bulletinStoreMap = new ConcurrentHashMap<>();
    private final BulletinProcessingStrategy processingStrategy = new DefaultBulletinProcessingStrategy();
    private final AtomicLong maxId = new AtomicLong(-1L);

    @Override
    public void addBulletin(final Bulletin bulletin) {
        processingStrategy.update(bulletin);

        final long bulletinId = bulletin.getId();
        maxId.getAndAccumulate(bulletinId, Math::max);
    }

    @Override
    public long getMaxBulletinId() {
        return maxId.get();
    }

    @Override
    public int getControllerBulletinCapacity() {
        return CONTROLLER_BUFFER_SIZE;
    }

    @Override
    public int getComponentBulletinCapacity() {
        return COMPONENT_BUFFER_SIZE;
    }

    @Override
    public List<Bulletin> findBulletins(final BulletinQuery bulletinQuery) {
        final Filter<Bulletin> filter = createFilter(bulletinQuery);

        final Set<Bulletin> selected = new TreeSet<>();
        int max = bulletinQuery.getLimit() == null ? Integer.MAX_VALUE : bulletinQuery.getLimit();

        for (final ConcurrentMap<String, RingBuffer<Bulletin>> componentMap : bulletinStoreMap.values()) {
            for (final RingBuffer<Bulletin> ringBuffer : componentMap.values()) {
                final List<Bulletin> bulletinsForComponent = ringBuffer.getSelectedElements(filter, max);
                selected.addAll(bulletinsForComponent);
                max -= bulletinsForComponent.size();
                if (max <= 0) {
                    break;
                }
            }
        }

        return new ArrayList<>(selected);
    }

    private Filter<Bulletin> createFilter(final BulletinQuery bulletinQuery) {
        final long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);

        return bulletin -> {
            // only include bulletins after the specified id
            if (bulletinQuery.getAfter() != null && bulletin.getId() <= bulletinQuery.getAfter()) {
                return false;
            }

            // if group pattern was specified see if it should be excluded
            if (bulletinQuery.getGroupIdPattern() != null) {
                // exclude if this bulletin doesnt have a group or if it doesnt match
                if (bulletin.getGroupId() == null || !bulletinQuery.getGroupIdPattern().matcher(bulletin.getGroupId()).find()) {
                    return false;
                }
            }

            // if a message pattern was specified see if it should be excluded
            if (bulletinQuery.getMessagePattern() != null) {
                // exclude if this bulletin doesnt have a message or if it doesnt match
                if (bulletin.getMessage() == null || !bulletinQuery.getMessagePattern().matcher(bulletin.getMessage()).find()) {
                    return false;
                }
            }

            // if a name pattern was specified see if it should be excluded
            if (bulletinQuery.getNamePattern() != null) {
                // exclude if this bulletin doesnt have a source name or if it doesnt match
                if (bulletin.getSourceName() == null || !bulletinQuery.getNamePattern().matcher(bulletin.getSourceName()).find()) {
                    return false;
                }
            }

            if (bulletin.getTimestamp().getTime() < fiveMinutesAgo) {
                return false;
            }

            // if a source id was specified see if it should be excluded
            if (bulletinQuery.getSourceIdPattern() != null) {
                // exclude if this bulletin doesn't have a source id or if it doesn't match
                if (bulletin.getSourceId() == null || !bulletinQuery.getSourceIdPattern().matcher(bulletin.getSourceId()).find()) {
                    return false;
                }
            }

            // if a source component type was specified see if it should be excluded
            if (bulletinQuery.getSourceType() != null) {
                // exclude if this bulletin source type doesn't match
                if (bulletin.getSourceType() == null || !bulletinQuery.getSourceType().equals(bulletin.getSourceType())) {
                    return false;
                }
            }

            return true;
        };
    }

    @Override
    public List<Bulletin> findBulletinsForSource(final String sourceId, final String groupId) {
        final BulletinQuery bulletinQuery = new BulletinQuery.Builder().sourceIdMatches(Pattern.quote(sourceId)).groupIdMatches(Pattern.quote(groupId)).limit(COMPONENT_BUFFER_SIZE).build();
        final ConcurrentMap<String, RingBuffer<Bulletin>> componentMap = bulletinStoreMap.get(groupId);
        if (componentMap == null) {
            return Collections.emptyList();
        }

        return findBulletinsForSource(sourceId, bulletinQuery, Collections.singleton(componentMap));
    }

    @Override
    public List<Bulletin> findBulletinsForSource(final String sourceId) {
        final BulletinQuery bulletinQuery = new BulletinQuery.Builder().sourceIdMatches(Pattern.quote(sourceId)).limit(COMPONENT_BUFFER_SIZE).build();
        return findBulletinsForSource(sourceId, bulletinQuery, this.bulletinStoreMap.values());
    }

    private List<Bulletin> findBulletinsForSource(final String sourceId, final BulletinQuery bulletinQuery, final Collection<ConcurrentMap<String, RingBuffer<Bulletin>>> bulletinStoreMaps) {
        final Filter<Bulletin> filter = createFilter(bulletinQuery);

        final int max = bulletinQuery.getLimit() == null ? Integer.MAX_VALUE : bulletinQuery.getLimit();
        for (final ConcurrentMap<String, RingBuffer<Bulletin>> componentMap : bulletinStoreMaps) {
            final RingBuffer<Bulletin> ringBuffer = componentMap.get(sourceId);
            if (ringBuffer == null) {
                continue;
            }

            final List<Bulletin> bulletinsForComponent = ringBuffer.getSelectedElements(filter, max);
            Collections.sort(bulletinsForComponent);
            return bulletinsForComponent;
        }

        return Collections.emptyList();
    }

    @Override
    public List<Bulletin> findBulletinsForGroupBySource(String groupId) {
        return findBulletinsForGroupBySource(groupId, COMPONENT_BUFFER_SIZE);
    }

    @Override
    public List<Bulletin> findBulletinsForGroupBySource(final String groupId, final int maxPerComponent) {
        final long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);

        final ConcurrentMap<String, RingBuffer<Bulletin>> componentMap = bulletinStoreMap.get(groupId);
        if (componentMap == null) {
            return Collections.emptyList();
        }

        final List<Bulletin> allComponentBulletins = new ArrayList<>();
        for (final RingBuffer<Bulletin> ringBuffer : componentMap.values()) {
            allComponentBulletins.addAll(ringBuffer.getSelectedElements(bulletin -> bulletin.getTimestamp().getTime() >= fiveMinutesAgo, maxPerComponent));
        }

        return allComponentBulletins;
    }

    @Override
    public List<Bulletin> findBulletinsForController() {
        return findBulletinsForController(CONTROLLER_BUFFER_SIZE);
    }

    @Override
    public List<Bulletin> findBulletinsForController(final int max) {
        final long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);

        final Filter<Bulletin> filter = bulletin -> bulletin.getTimestamp().getTime() >= fiveMinutesAgo;

        final List<Bulletin> controllerBulletins = new ArrayList<>();

        final ConcurrentMap<String, RingBuffer<Bulletin>> controllerBulletinMap = bulletinStoreMap.get(CONTROLLER_BULLETIN_STORE_KEY);
        if (controllerBulletinMap != null) {
            final RingBuffer<Bulletin> buffer = controllerBulletinMap.get(CONTROLLER_BULLETIN_STORE_KEY);
            if (buffer != null) {
                controllerBulletins.addAll(buffer.getSelectedElements(filter, max));
            }
        }

        // We only want the newest bulletin, so we sort based on time and take the top 'max' entries
        Collections.sort(controllerBulletins);
        if (controllerBulletins.size() > max) {
            return controllerBulletins.subList(0, max);
        }

        return controllerBulletins;
    }

    private List<RingBuffer<Bulletin>> getBulletinBuffers(final Bulletin bulletin) {
        final String storageKey = getBulletinStoreKey(bulletin);

        ConcurrentMap<String, RingBuffer<Bulletin>> componentMap = bulletinStoreMap.get(storageKey);
        if (componentMap == null) {
            componentMap = new ConcurrentHashMap<>();
            final ConcurrentMap<String, RingBuffer<Bulletin>> existing = bulletinStoreMap.putIfAbsent(storageKey, componentMap);
            if (existing != null) {
                componentMap = existing;
            }
        }

        final List<RingBuffer<Bulletin>> buffers = new ArrayList<>(2);

        if (isControllerBulletin(bulletin)) {
            RingBuffer<Bulletin> bulletinBuffer = componentMap.get(CONTROLLER_BULLETIN_STORE_KEY);
            if (bulletinBuffer == null) {
                bulletinBuffer = new RingBuffer<>(CONTROLLER_BUFFER_SIZE);
                final RingBuffer<Bulletin> existingBuffer = componentMap.putIfAbsent(CONTROLLER_BULLETIN_STORE_KEY, bulletinBuffer);
                if (existingBuffer != null) {
                    bulletinBuffer = existingBuffer;
                }
            }

            buffers.add(bulletinBuffer);
        }

        if (bulletin.getSourceType() != ComponentType.FLOW_CONTROLLER) {
            RingBuffer<Bulletin> bulletinBuffer = componentMap.get(bulletin.getSourceId());
            if (bulletinBuffer == null) {
                bulletinBuffer = new RingBuffer<>(COMPONENT_BUFFER_SIZE);
                final RingBuffer<Bulletin> existingBuffer = componentMap.putIfAbsent(bulletin.getSourceId(), bulletinBuffer);
                if (existingBuffer != null) {
                    bulletinBuffer = existingBuffer;
                }
            }

            buffers.add(bulletinBuffer);
        }

        return buffers;
    }

    private String getBulletinStoreKey(final Bulletin bulletin) {
        switch (bulletin.getSourceType()) {
            case FLOW_CONTROLLER:
                return CONTROLLER_BULLETIN_STORE_KEY;
            case CONTROLLER_SERVICE:
                return SERVICE_BULLETIN_STORE_KEY;
            case REPORTING_TASK:
                return REPORTING_TASK_BULLETIN_STORE_KEY;
            case FLOW_ANALYSIS_RULE:
                return FLOW_ANALYSIS_RULE_BULLETIN_STORE_KEY;
            case PARAMETER_PROVIDER:
                return PARAMETER_PROVIDER_BULLETIN_STORE_KEY;
            case FLOW_REGISTRY_CLIENT:
                return FLOW_REGISTRY_CLIENT_STORE_KEY;
            default:
                return bulletin.getGroupId();
        }
    }

    private boolean isControllerBulletin(final Bulletin bulletin) {
        switch (bulletin.getSourceType()) {
            case FLOW_CONTROLLER:
            case CONTROLLER_SERVICE:
            case REPORTING_TASK:
            case FLOW_ANALYSIS_RULE:
            case PARAMETER_PROVIDER:
            case FLOW_REGISTRY_CLIENT:
                return true;
            default:
                return false;
        }
    }

    private class DefaultBulletinProcessingStrategy implements BulletinProcessingStrategy {

        @Override
        public void update(final Bulletin bulletin) {
            for (final RingBuffer<Bulletin> bulletinBuffer : getBulletinBuffers(bulletin)) {
                bulletinBuffer.add(bulletin);
            }
        }
    }
}
