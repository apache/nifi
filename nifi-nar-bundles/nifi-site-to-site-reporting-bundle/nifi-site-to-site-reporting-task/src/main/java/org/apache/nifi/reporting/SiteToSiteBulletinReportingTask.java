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

package org.apache.nifi.reporting;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.scheduling.SchedulingStrategy;

@Tags({"bulletin", "site", "site to site", "restricted"})
@CapabilityDescription("Publishes Bulletin events using the Site To Site protocol. Note: only up to 5 bulletins are stored per component and up to "
        + "10 bulletins at controller level for a duration of up to 5 minutes. If this reporting task is not scheduled frequently enough some bulletins "
        + "may not be sent.")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last bulletin ID so that on restart the task knows where it left off.")
@Restricted("Provides operator the ability to send sensitive details contained in bulletin events to any external system.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class SiteToSiteBulletinReportingTask extends AbstractSiteToSiteReportingTask {

    static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    static final String LAST_EVENT_ID_KEY = "last_event_id";

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
        .name("Platform")
        .description("The value to use for the platform field in each provenance event.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("nifi")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private volatile long lastSentBulletinId = -1L;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PLATFORM);
        properties.remove(BATCH_SIZE);
        return properties;
    }

    @Override
    public void onTrigger(final ReportingContext context) {

        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug("This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                + "Will wait for Node Identifier to be established.");
            return;
        }

        if (lastSentBulletinId < 0) {
            Map<String, String> state;
            try {
                state = context.getStateManager().getState(Scope.LOCAL).toMap();
            } catch (IOException e) {
                getLogger().error("Failed to get state at start up due to:" + e.getMessage(), e);
                return;
            }
            if (state.containsKey(LAST_EVENT_ID_KEY)) {
                lastSentBulletinId = Long.parseLong(state.get(LAST_EVENT_ID_KEY));
            }
        }

        final BulletinQuery bulletinQuery = new BulletinQuery.Builder().after(lastSentBulletinId).build();
        final List<Bulletin> bulletins = context.getBulletinRepository().findBulletins(bulletinQuery);

        if(bulletins == null || bulletins.isEmpty()) {
            getLogger().debug("No events to send because no events are stored in the repository.");
            return;
        }

        final OptionalLong opMaxId = bulletins.stream().mapToLong(t -> t.getId()).max();
        final Long currMaxId = opMaxId.isPresent() ? opMaxId.getAsLong() : -1;

        if(currMaxId < lastSentBulletinId){
            getLogger().warn("Current bulletin max id is {} which is less than what was stored in state as the last queried event, which was {}. "
                    + "This means the bulletins repository restarted its ids. Restarting querying from the beginning.", new Object[]{currMaxId, lastSentBulletinId});
            lastSentBulletinId = -1;
        }

        if (currMaxId == lastSentBulletinId) {
            getLogger().debug("No events to send due to the current max id being equal to the last id that was sent.");
            return;
        }

        final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();

        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();

        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("Z"));

        final long start = System.nanoTime();

        // Create a JSON array of all the events in the current batch
        final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
        for (final Bulletin bulletin : bulletins) {
            if(bulletin.getId() > lastSentBulletinId) {
                arrayBuilder.add(serialize(factory, builder, bulletin, df, platform, nodeId));
            }
        }
        final JsonArray jsonArray = arrayBuilder.build();

        // Send the JSON document for the current batch
        try {
            final Transaction transaction = getClient().createTransaction(TransferDirection.SEND);
            if (transaction == null) {
                getLogger().info("All destination nodes are penalized; will attempt to send data later");
                return;
            }

            final Map<String, String> attributes = new HashMap<>();
            final String transactionId = UUID.randomUUID().toString();
            attributes.put("reporting.task.transaction.id", transactionId);
            attributes.put("mime.type", "application/json");

            final byte[] data = jsonArray.toString().getBytes(StandardCharsets.UTF_8);
            transaction.send(data, attributes);
            transaction.confirm();
            transaction.complete();

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            getLogger().info("Successfully sent {} Bulletins to destination in {} ms; Transaction ID = {}; First Event ID = {}",
                    new Object[]{bulletins.size(), transferMillis, transactionId, bulletins.get(0).getId()});
        } catch (final IOException e) {
            throw new ProcessException("Failed to send Bulletins to destination due to IOException:" + e.getMessage(), e);
        }

        // Store the id of the last event so we know where we left off
        try {
            context.getStateManager().setState(Collections.singletonMap(LAST_EVENT_ID_KEY, String.valueOf(currMaxId)), Scope.LOCAL);
        } catch (final IOException ioe) {
            getLogger().error("Failed to update state to {} due to {}; this could result in events being re-sent after a restart.",
                    new Object[]{currMaxId, ioe.getMessage()}, ioe);
        }

        lastSentBulletinId = currMaxId;
    }

    static JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder, final Bulletin bulletin, final DateFormat df,
        final String platform, final String nodeIdentifier) {

        addField(builder, "objectId", UUID.randomUUID().toString());
        addField(builder, "platform", platform);
        addField(builder, "bulletinId", bulletin.getId());
        addField(builder, "bulletinCategory", bulletin.getCategory());
        addField(builder, "bulletinGroupId", bulletin.getGroupId());
        addField(builder, "bulletinGroupName", bulletin.getGroupName());
        addField(builder, "bulletinLevel", bulletin.getLevel());
        addField(builder, "bulletinMessage", bulletin.getMessage());
        addField(builder, "bulletinNodeAddress", bulletin.getNodeAddress());
        addField(builder, "bulletinNodeId", nodeIdentifier);
        addField(builder, "bulletinSourceId", bulletin.getSourceId());
        addField(builder, "bulletinSourceName", bulletin.getSourceName());
        addField(builder, "bulletinSourceType", bulletin.getSourceType() == null ? null : bulletin.getSourceType().name());
        addField(builder, "bulletinTimestamp", df.format(bulletin.getTimestamp()));

        return builder.build();
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final Long value) {
        if (value != null) {
            builder.add(key, value.longValue());
        }
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final String value) {
        if (value == null) {
            return;
        }
        builder.add(key, value);
    }

}
