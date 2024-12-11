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
package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.FlowRegistryBucketDTO;
import org.apache.nifi.web.api.entity.FlowRegistryBucketEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBucketsEntity;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result for a FlowRegistryBucketsEntity.
 */
public class RegistryBucketsResult extends AbstractWritableResult<FlowRegistryBucketsEntity> {

    final FlowRegistryBucketsEntity bucketsEntity;

    public RegistryBucketsResult(final ResultType resultType, final FlowRegistryBucketsEntity bucketsEntity) {
        super(resultType);
        this.bucketsEntity = Objects.requireNonNull(bucketsEntity);
    }

    @Override
    public FlowRegistryBucketsEntity getResult() {
        return this.bucketsEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final Set<FlowRegistryBucketEntity> buckets = bucketsEntity.getBuckets();
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        final List<FlowRegistryBucketDTO> bucketsDTO = buckets.stream()
            .map(b -> b.getBucket())
            .sorted(Comparator.comparing(FlowRegistryBucketDTO::getName))
            .toList();

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Description", 11, 40, true)
                .build();

        for (int i = 0; i < bucketsDTO.size(); i++) {
            FlowRegistryBucketDTO bucket = bucketsDTO.get(i);
            table.addRow("" + (i + 1), bucket.getName(), bucket.getId(), bucket.getDescription() == null ? "" : bucket.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
