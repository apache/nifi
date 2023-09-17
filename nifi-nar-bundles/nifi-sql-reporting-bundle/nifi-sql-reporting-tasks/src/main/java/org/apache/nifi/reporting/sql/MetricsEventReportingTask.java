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
package org.apache.nifi.reporting.sql;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.sql.util.QueryMetricsUtil;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.PropertyContextActionHandler;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.reporting.sql.util.TrackedQueryTime.BULLETIN_END_TIME;
import static org.apache.nifi.reporting.sql.util.TrackedQueryTime.BULLETIN_START_TIME;
import static org.apache.nifi.reporting.sql.util.TrackedQueryTime.PROVENANCE_END_TIME;
import static org.apache.nifi.reporting.sql.util.TrackedQueryTime.PROVENANCE_START_TIME;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE;

@Tags({"reporting", "rules", "action", "action handler", "status", "connection", "processor", "jvm", "metrics", "history", "bulletin", "sql"})
@CapabilityDescription("Triggers rules-driven actions based on metrics values ")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last execution time so that on restart the task knows where it left off.")
public class MetricsEventReportingTask extends AbstractReportingTask implements QueryTimeAware {

    private List<PropertyDescriptor> properties;
    private MetricsQueryService metricsQueryService;
    private volatile RulesEngineService rulesEngineService;
    private volatile PropertyContextActionHandler actionHandler;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ReportingInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QueryMetricsUtil.QUERY);
        properties.add(QueryMetricsUtil.RULES_ENGINE);
        properties.add(QueryMetricsUtil.ACTION_HANDLER);
        properties.add(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION);
        properties.add(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) {
        actionHandler = context.getProperty(QueryMetricsUtil.ACTION_HANDLER).asControllerService(PropertyContextActionHandler.class);
        rulesEngineService = context.getProperty(QueryMetricsUtil.RULES_ENGINE).asControllerService(RulesEngineService.class);
        final Integer defaultPrecision = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();
        final Integer defaultScale = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE).evaluateAttributeExpressions().asInteger();
        metricsQueryService = new MetricsSqlQueryService(getLogger(), defaultPrecision, defaultScale);
    }

    @Override
    public void onTrigger(ReportingContext context) {
        String sql = context.getProperty(QueryMetricsUtil.QUERY).evaluateAttributeExpressions().getValue();
        try {
            sql = processStartAndEndTimes(context, sql, BULLETIN_START_TIME, BULLETIN_END_TIME);
            sql = processStartAndEndTimes(context, sql, PROVENANCE_START_TIME, PROVENANCE_END_TIME);

            fireRules(context, actionHandler, rulesEngineService, sql);
        } catch (Exception e) {
            getLogger().error("Error opening loading rules: {}", e.getMessage(), e);
        }
    }

    private void fireRules(ReportingContext context, PropertyContextActionHandler actionHandler, RulesEngineService engine, String query) throws Exception {
        getLogger().debug("Executing query: {}", query);
        QueryResult queryResult = metricsQueryService.query(context, query);
        ResultSetRecordSet recordSet = metricsQueryService.getResultSetRecordSet(queryResult);
        Record record;
        try {
            while ((record = recordSet.next()) != null) {
                final Map<String, Object> facts = new HashMap<>();
                for (String fieldName : record.getRawFieldNames()) {
                    facts.put(fieldName, record.getValue(fieldName));
                }
                List<Action> actions = engine.fireRules(facts);
                if (actions == null || actions.isEmpty()) {
                    getLogger().debug("No actions required for provided facts.");
                } else {
                    actions.forEach(action -> actionHandler.execute(context, action, facts));
                }
            }
        } finally {
            metricsQueryService.closeQuietly(recordSet);
        }
    }
}