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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.FlowAnalysisRuleResult;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for retrieving a flow analysis rule.
 */
public class GetFlowAnalysisRule extends AbstractNiFiCommand<FlowAnalysisRuleResult> {

    public GetFlowAnalysisRule() {
        super("get-flow-analysis-rule", FlowAnalysisRuleResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves a flow analysis rule.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.FAR_ID.createOption());
    }

    @Override
    public FlowAnalysisRuleResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String ruleId = getRequiredArg(properties, CommandOption.FAR_ID);
        final ControllerClient controllerClient = client.getControllerClient();

        final FlowAnalysisRuleEntity ruleEntity = controllerClient.getFlowAnalysisRule(ruleId);
        return new FlowAnalysisRuleResult(getResultType(properties), ruleEntity);
    }
}
