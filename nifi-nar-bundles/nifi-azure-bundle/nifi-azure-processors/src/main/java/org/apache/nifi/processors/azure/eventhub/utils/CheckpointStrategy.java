package org.apache.nifi.processors.azure.eventhub.utils;

import org.apache.nifi.components.DescribedValue;

public enum CheckpointStrategy implements DescribedValue {
    AZURE_BLOB_STORAGE("azure-blob-storage", "Use Azure Blob Storage to store partition checkpoints and ownership"),
    COMPONENT_STATE("component-state", "Use component state to store partition checkpoints");

    private final String label;
    private final String description;

    CheckpointStrategy(String label, String description) {
        this.label = label;
        this.description = description;
    }

    @Override
    public String getValue() {
        return this.name();
    }

    @Override
    public String getDisplayName() {
        return label;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

