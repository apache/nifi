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
package org.apache.nifi.processors.adx.sink.enums;

public enum IngestionMappingKindEnum {
    IM_KIND_APACHEAVRO("IngestionsMappingKind:Avro","Ingestion Mapping Kind (Type) is Avro."),
    IM_KIND_AVRO("IngestionsMappingKind:Avro","Ingestion Mapping Kind (Type) is Avro."),
    IM_KIND_CSV("IngestionsMappingKind:Csv","Ingestion Mapping Kind (Type) is Csv."),
    IM_KIND_JSON("IngestionsMappingKind:Json","Ingestion Mapping Kind (Type) is Json."),
    IM_KIND_PARQUET("IngestionsMappingKind:Parquet","Ingestion Mapping Kind (Type) is Parquet."),
    IM_KIND_ORC("IngestionsMappingKind:Orc","Ingestion Mapping Kind (Type) is Orc.");

    private String mappingKind;
    private String description;


    IngestionMappingKindEnum(String mappingKind, String description) {
        this.mappingKind = mappingKind;
        this.description = description;
    }

    public String getMappingKind() {
        return mappingKind;
    }

    public String getDescription() {
        return description;
    }
}
