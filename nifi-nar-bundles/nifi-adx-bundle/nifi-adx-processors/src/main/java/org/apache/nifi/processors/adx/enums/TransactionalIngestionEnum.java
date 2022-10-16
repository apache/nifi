package org.apache.nifi.processors.adx.enums;

public enum TransactionalIngestionEnum {
    YES("Yes","Transactionality required for ingestion"),
    NO("No","Transactionality not required for ingestion");

    private String transactionalIngestion;
    private String description;

    TransactionalIngestionEnum(String transactionalIngestion, String description) {
        this.transactionalIngestion = transactionalIngestion;
        this.description = description;
    }

    public String getTransactionalIngestion() {
        return transactionalIngestion;
    }

    public void setTransactionalIngestion(String transactionalIngestion) {
        this.transactionalIngestion = transactionalIngestion;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
