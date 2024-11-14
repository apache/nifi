/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.command.nifi.flow;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiActivateCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command for enabling flow analysis rule.
 */
public class EnableFlowAnalysisRules extends AbstractNiFiActivateCommand<FlowAnalysisRuleEntity, FlowAnalysisRuleRunStatusEntity> {

    public EnableFlowAnalysisRules() {
        super("enable-flow-analysis-rules");
    }

    @Override
    public String getDescription() {
        return "Attempts to enable one or all flow analysis rule(s). In stand-alone mode this command " +
                "will not produce all of the output seen in interactive mode unless the --verbose argument is specified.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.FAR_ID.createOption());
    }

    @Override
    public VoidResult doExecute(
            final NiFiClient client,
            final Properties properties) throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String ruleId = getArg(properties, CommandOption.FAR_ID);
        final Set<FlowAnalysisRuleEntity> ruleEntities = new HashSet<>();

        if (StringUtils.isBlank(ruleId)) {
            final FlowAnalysisRulesEntity rulesEntity = client.getControllerClient().getFlowAnalysisRules();
            ruleEntities.addAll(rulesEntity.getFlowAnalysisRules());
        } else {
            ruleEntities.add(client.getControllerClient().getFlowAnalysisRule(ruleId));
        }

        activate(client, properties, ruleEntities, "ENABLED");

        return VoidResult.getInstance();
    }

    @Override
    public FlowAnalysisRuleRunStatusEntity getRunStatusEntity() {
        return new FlowAnalysisRuleRunStatusEntity();
    }

    @Override
    public FlowAnalysisRuleEntity activateComponent(
            final NiFiClient client,
            final FlowAnalysisRuleEntity ruleEntity,
            final FlowAnalysisRuleRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        return client.getControllerClient().activateFlowAnalysisRule(ruleEntity.getId(), runStatusEntity);
    }

    @Override
    public String getDispName(final FlowAnalysisRuleEntity ruleEntity) {
        return "Flow analysis rule \"" + ruleEntity.getComponent().getName() + "\" " +
                "(id: " + ruleEntity.getId() + ")";
    }
}
