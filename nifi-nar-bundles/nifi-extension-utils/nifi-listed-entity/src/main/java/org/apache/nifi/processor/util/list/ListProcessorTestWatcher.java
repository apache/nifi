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
package org.apache.nifi.processor.util.list;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class provides a way to dump list-able entities, processor state and transferred FlowFiles into 'success' relationship,
 * which is useful to debug test issues especially at automation test environment such as Travis that is difficult to debug.
 */
public class ListProcessorTestWatcher implements TestWatcher, BeforeEachCallback {

    private static final Logger logger = LoggerFactory.getLogger(ListProcessorTestWatcher.class);
    private static final Consumer<String> logStateDump = logger::info;

    @FunctionalInterface
    public interface Provider<T> {
        T provide();
    }

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private final Provider<Map<String, String>> stateMapProvider;
    private final Provider<List<ListableEntity>> entitiesProvider;
    private final Provider<List<FlowFile>> successFlowFilesProvider;

    private long startedAtMillis;

    public ListProcessorTestWatcher(Provider<Map<String, String>> stateMapProvider, Provider<List<ListableEntity>> entitiesProvider, Provider<List<FlowFile>> successFlowFilesProvider) {
        this.stateMapProvider = stateMapProvider;
        this.entitiesProvider = entitiesProvider;
        this.successFlowFilesProvider = successFlowFilesProvider;
    }

    private void log(Consumer<String> dumper, String format, Object ... args) {
        dumper.accept(String.format(format, args));
    }

    public void dumpState(final long start) {
        dumpState(logStateDump, stateMapProvider.provide(), entitiesProvider.provide(), successFlowFilesProvider.provide(), start);
    }

    private void dumpState(Consumer<String> d, final Map<String, String> state, final List<ListableEntity> entities, final List<FlowFile> flowFiles, final long start) {

        final OffsetDateTime nTime = OffsetDateTime.now();
        log(d, "--------------------------------------------------------------------");
        log(d, "%-19s   %-13s %-23s %s", "", "timestamp", "date from timestamp", "t0 delta");
        log(d, "%-19s   %-13s %-23s %s", "-------------------", "-------------", "-----------------------", "--------");
        log(d, "%-19s = %13d %s %8d", "started at", start, dateTimeFormatter.format(Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault())), 0);
        log(d, "%-19s = %13d %s %8d", "current time", nTime.toInstant().toEpochMilli(), dateTimeFormatter.format(nTime), 0);
        log(d, "---- processor state -----------------------------------------------");
        if (state.containsKey("processed.timestamp")) {
            final long pTime = Long.parseLong(state.get("processed.timestamp"));
            final OffsetDateTime processedTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(pTime), ZoneOffset.UTC);
            log(d, "%19s = %13d %s %8d", "processed.timestamp", pTime, dateTimeFormatter.format(processedTime), pTime - nTime.toInstant().toEpochMilli());
        } else {
            log(d, "%19s = na", "processed.timestamp");
        }
        if (state.containsKey("listing.timestamp")) {
            final long lTime = Long.parseLong(state.get("listing.timestamp"));
            log(d, "%19s = %13d %s %8d", "listing.timestamp", lTime, dateTimeFormatter.format(Instant.ofEpochMilli(lTime).atZone(ZoneId.systemDefault())), lTime - nTime.toInstant().toEpochMilli());
        } else {
            log(d, "%19s = na", "listing.timestamp");
        }
        log(d, "---- input folder contents -----------------------------------------");
        entities.sort(Comparator.comparing(ListableEntity::getIdentifier));
        for (ListableEntity entity : entities) {
            final OffsetDateTime timestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(entity.getTimestamp()), ZoneId.systemDefault());
            log(d, "%19s = %12d %s %8d", entity.getIdentifier(), entity.getTimestamp(), dateTimeFormatter.format(timestamp), entity.getTimestamp() - nTime.toInstant().toEpochMilli());
        }
        log(d, "---- output flowfiles ----------------------------------------------");
        final Map<String, Long> fileTimes = entities.stream().collect(Collectors.toMap(ListableEntity::getIdentifier, ListableEntity::getTimestamp));
        for (FlowFile ff : flowFiles) {
            String fName = ff.getAttribute(CoreAttributes.FILENAME.key());
            Long fTime = fileTimes.get(fName);
            final OffsetDateTime timestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(fTime), ZoneId.systemDefault());
            log(d, "%19s = %13d %s %8d", fName, fTime, dateTimeFormatter.format(timestamp), fTime - nTime.toInstant().toEpochMilli());
        }
        log(d, "REL_SUCCESS count = " + flowFiles.size());
        log(d, "--------------------------------------------------------------------");
        log(d, "");
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        startedAtMillis = System.currentTimeMillis();
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        if (!(cause instanceof AssertionError)) {
            return;
        }
        dumpState(startedAtMillis);
    }
}
