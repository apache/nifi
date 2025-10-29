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

import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.scheduling.SchedulingStrategy;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonBuilderFactory;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tags({"bulletin", "site", "site to site"})
@CapabilityDescription("Publishes Bulletin events using the Site To Site protocol. Note: only up to 5 bulletins are stored per component and up to "
        + "10 bulletins at controller level for a duration of up to 5 minutes. If this reporting task is not scheduled frequently enough some bulletins "
        + "may not be sent.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXPORT_NIFI_DETAILS,
                        explanation = "Provides operator the ability to send sensitive details contained in bulletin events to any external system.")
        }
)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class SiteToSiteBulletinReportingTask extends AbstractSiteToSiteReportingTask {

    private volatile long lastSentBulletinId = -1L;

    public SiteToSiteBulletinReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-bulletins.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SiteToSiteUtils.PLATFORM);
        properties.remove(SiteToSiteUtils.BATCH_SIZE);
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

        final BulletinQuery bulletinQuery = new BulletinQuery.Builder().after(lastSentBulletinId).build();
        final List<Bulletin> bulletins = context.getBulletinRepository().findBulletins(bulletinQuery);

        if (bulletins == null || bulletins.isEmpty()) {
            getLogger().debug("No events to send because no events are stored in the repository.");
            return;
        }

        final OptionalLong opMaxId = bulletins.stream().mapToLong(Bulletin::getId).max();
        final long currMaxId = opMaxId.isPresent() ? opMaxId.getAsLong() : -1;

        if (currMaxId < lastSentBulletinId) {
            getLogger().warn("Current bulletin max id is {} which is less than what was stored in state as the last queried event, which was {}. "
                    + "This means the bulletins repository restarted its ids. Restarting querying from the beginning.", currMaxId, lastSentBulletinId);
            lastSentBulletinId = -1;
        }

        if (currMaxId == lastSentBulletinId) {
            getLogger().debug("No events to send due to the current max id being equal to the last id that was sent.");
            return;
        }

        final String platform = context.getProperty(SiteToSiteUtils.PLATFORM).evaluateAttributeExpressions().getValue();
        final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();

        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();

        final long start = System.nanoTime();

        // Create a JSON array of all the events in the current batch
        final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
        for (final Bulletin bulletin : bulletins) {
            if (bulletin.getId() > lastSentBulletinId) {
                arrayBuilder.add(serialize(builder, bulletin, platform, nodeId, allowNullValues));
            }
        }
        final JsonArray jsonArray = arrayBuilder.build();

        // Send the JSON document for the current batch
        Transaction transaction = null;
        try {
            // Lazily create SiteToSiteClient to provide a StateManager
            setup(context);

            transaction = getClient().createTransaction(TransferDirection.SEND);
            if (transaction == null) {
                getLogger().info("All destination nodes are penalized; will attempt to send data later");
                return;
            }

            final Map<String, String> attributes = new HashMap<>();
            final String transactionId = UUID.randomUUID().toString();
            attributes.put("reporting.task.transaction.id", transactionId);
            attributes.put("reporting.task.name", getName());
            attributes.put("reporting.task.uuid", getIdentifier());
            attributes.put("reporting.task.type", this.getClass().getSimpleName());
            attributes.put("mime.type", "application/json");

            sendData(context, transaction, attributes, jsonArray);
            transaction.confirm();
            transaction.complete();

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            getLogger().info("Successfully sent {} Bulletins to destination in {} ms; Transaction ID = {}; First Event ID = {}",
                    bulletins.size(), transferMillis, transactionId, bulletins.get(0).getId());
        } catch (final Exception e) {
            if (transaction != null) {
                transaction.error();
            }
            if (e instanceof ProcessException) {
                throw (ProcessException) e;
            } else {
                throw new ProcessException("Failed to send Bulletins to destination due to IOException:" + e.getMessage(), e);
            }
        }

        lastSentBulletinId = currMaxId;
    }

    private JsonObject serialize(final JsonObjectBuilder builder, final Bulletin bulletin,
                                 final String platform, final String nodeIdentifier, Boolean allowNullValues) {

        addField(builder, "objectId", UUID.randomUUID().toString(), allowNullValues);
        addField(builder, "platform", platform, allowNullValues);
        addField(builder, "bulletinId", bulletin.getId(), allowNullValues);
        addField(builder, "bulletinCategory", bulletin.getCategory(), allowNullValues);
        addField(builder, "bulletinGroupId", bulletin.getGroupId(), allowNullValues);
        addField(builder, "bulletinGroupName", bulletin.getGroupName(), allowNullValues);
        addField(builder, "bulletinGroupPath", bulletin.getGroupPath(), allowNullValues);
        addField(builder, "bulletinLevel", bulletin.getLevel(), allowNullValues);
        addField(builder, "bulletinMessage", bulletin.getMessage(), allowNullValues);
        addField(builder, "bulletinNodeAddress", bulletin.getNodeAddress(), allowNullValues);
        addField(builder, "bulletinNodeId", nodeIdentifier, allowNullValues);
        addField(builder, "bulletinSourceId", bulletin.getSourceId(), allowNullValues);
        addField(builder, "bulletinSourceName", bulletin.getSourceName(), allowNullValues);
        addField(builder, "bulletinSourceType", bulletin.getSourceType() == null ? null : bulletin.getSourceType().name(), allowNullValues);
        addField(builder, "bulletinTimestamp", DATE_TIME_FORMATTER.format(bulletin.getTimestamp().toInstant()), allowNullValues);
        addField(builder, "bulletinFlowFileUuid", bulletin.getFlowFileUuid(), allowNullValues);

        return builder.build();
    }

}
