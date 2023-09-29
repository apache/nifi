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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Collections;
import java.util.List;

class NiFiTableScan extends TableScan implements EnumerableRel {
    private final ConstantExpression fieldExpression;
    private final int[] fields;

    protected NiFiTableScan(final RelOptCluster cluster, final RelOptTable table, final int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), Collections.emptyList(), table);
        this.fields = fields;
        fieldExpression = Expressions.constant(fields);
    }

    @Override
    public void register(final RelOptPlanner planner) {
        planner.addRule(new NiFiProjectTableScanRule(NiFiProjectTableScanRule.Config.DEFAULT));
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for (final int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    // Logic taken directly from the Calcite CSV Tutorial
    @Override
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        // Multiply the cost by a factor that makes a scan more attractive if it
        // has significantly fewer fields than the original scan.
        //
        // The "+ 2D" on top and bottom keeps the function fairly smooth.
        //
        // For example, if table has 3 fields, project has 1 field,
        // then factor = (1 + 2) / (3 + 2) = 0.6
        return super.computeSelfCost(planner, mq)
            .multiplyBy(((double) fields.length + 2D)
                / ((double) table.getRowType().getFieldCount() + 2D));
    }

    @Override
    public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final ParameterExpression rootExpression = implementor.getRootExpression();
        final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray());

        final Expression tableExpression = table.getExpression(CalciteTable.class);
        final MethodCallExpression methodCallExpression = Expressions.call(tableExpression, "project", rootExpression, fieldExpression);
        final BlockStatement blockStatement = Blocks.toBlock(methodCallExpression);
        return implementor.result(physType, blockStatement);
    }
}
