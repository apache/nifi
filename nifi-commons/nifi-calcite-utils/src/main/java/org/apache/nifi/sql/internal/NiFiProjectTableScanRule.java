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

package org.apache.nifi.sql.internal;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class NiFiProjectTableScanRule extends RelRule<NiFiProjectTableScanRule.Config> {

    NiFiProjectTableScanRule(final Config config) {
        super(config);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        final Project project = call.rel(0);

        // Attempt to locate NiFiTableScan as immediate input
        if (!(project.getInput() instanceof NiFiTableScan scan)) {
            return;
        }

        final int[] fields = getProjectionFields(project.getProjects());
        if (fields == null) {
            // Project contains expressions more complex than just field references.
            return;
        }

        final NiFiTableScan tableScan = new NiFiTableScan(scan.getCluster(), scan.getTable(), fields);
        call.transformTo(tableScan);
    }


    private static int[] getProjectionFields(final List<RexNode> expressions) {
        final int[] fields = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            final RexNode exp = expressions.get(i);

            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                // not a simple projection
                return null;
            }
        }

        return fields;
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = new StandardConfig()
            .withOperandSupplier(b0 -> b0.operand(Project.class).anyInputs());


        @Override
        default NiFiProjectTableScanRule toRule() {
            return new NiFiProjectTableScanRule(this);
        }
    }

    private static class StandardConfig implements Config {
        private RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
        private String description;
        private OperandTransform operandTransform;


        @Override
        public StandardConfig withRelBuilderFactory(final RelBuilderFactory factory) {
            this.relBuilderFactory = factory;
            return this;
        }

        @Override
        public StandardConfig withDescription(final String description) {
            this.description = description;
            return this;
        }

        @Override
        public StandardConfig withOperandSupplier(final OperandTransform transform) {
            this.operandTransform = transform;
            return this;
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            return relBuilderFactory;
        }

        @Override
        public String description() {
            return description;
        }


        @Override
        public OperandTransform operandSupplier() {
            return this.operandTransform;
        }
    }

}
