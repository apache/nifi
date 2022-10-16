package org.apache.nifi.processors.adx.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestionBatchingPolicy {

    @JsonProperty(value = "MaximumBatchingTimeSpan")
    private String maximumBatchingTimeSpan;
    @JsonProperty(value = "MaximumNumberOfItems")
    private int maximumNumberOfItems;
    @JsonProperty(value = "MaximumRawDataSizeMB")
    private int maximumRawDataSizeMB;

    public String getMaximumBatchingTimeSpan() {
        return maximumBatchingTimeSpan;
    }

    public void setMaximumBatchingTimeSpan(String maximumBatchingTimeSpan) {
        this.maximumBatchingTimeSpan = maximumBatchingTimeSpan;
    }

    public int getMaximumNumberOfItems() {
        return maximumNumberOfItems;
    }

    public void setMaximumNumberOfItems(int maximumNumberOfItems) {
        this.maximumNumberOfItems = maximumNumberOfItems;
    }

    public int getMaximumRawDataSizeMB() {
        return maximumRawDataSizeMB;
    }

    public void setMaximumRawDataSizeMB(int maximumRawDataSizeMB) {
        this.maximumRawDataSizeMB = maximumRawDataSizeMB;
    }
}
