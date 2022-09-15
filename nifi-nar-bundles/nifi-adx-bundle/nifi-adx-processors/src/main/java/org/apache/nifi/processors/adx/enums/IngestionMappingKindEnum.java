package org.apache.nifi.processors.adx.enums;

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
