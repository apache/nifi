package org.apache.nifi.processors.kafka.pubsub;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class PublishingContext {

    private final InputStream contentStream;

    private final String topic;

    private final int lastAckedMessageIndex;

    /*
     * We're using the default value from Kafka. We are using it to control the
     * message size before it goes to to Kafka thus limiting possibility of a
     * late failures in Kafka client.
     */
    private int maxRequestSize = 1048576; // kafka default

    private boolean maxRequestSizeSet;

    private byte[] keyBytes;

    private byte[] delimiterBytes;



    PublishingContext(InputStream contentStream, String topic) {
        this(contentStream, topic, -1);
    }

    PublishingContext(InputStream contentStream, String topic, int lastAckedMessageIndex) {
        this.validateInput(contentStream, topic, lastAckedMessageIndex);
        this.contentStream = contentStream;
        this.topic = topic;
        this.lastAckedMessageIndex = lastAckedMessageIndex;
    }

    @Override
    public String toString() {
        return "topic: '" + this.topic + "'; delimiter: '" + new String(this.delimiterBytes, StandardCharsets.UTF_8) + "'";
    }

    int getLastAckedMessageIndex() {
        return this.lastAckedMessageIndex;
    }

    int getMaxRequestSize() {
        return this.maxRequestSize;
    }

    byte[] getKeyBytes() {
        return this.keyBytes;
    }

    byte[] getDelimiterBytes() {
        return this.delimiterBytes;
    }

    InputStream getContentStream() {
        return this.contentStream;
    }

    String getTopic() {
        return this.topic;
    }

    void setKeyBytes(byte[] keyBytes) {
        if (this.keyBytes == null) {
            if (keyBytes != null) {
                this.assertBytesValid(keyBytes);
                this.keyBytes = keyBytes;
            }
        } else {
            throw new IllegalArgumentException("'keyBytes' can only be set once per instance");
        }
    }

    void setDelimiterBytes(byte[] delimiterBytes) {
        if (this.delimiterBytes == null) {
            if (delimiterBytes != null) {
                this.assertBytesValid(delimiterBytes);
                this.delimiterBytes = delimiterBytes;
            }
        } else {
            throw new IllegalArgumentException("'delimiterBytes' can only be set once per instance");
        }
    }

    void setMaxRequestSize(int maxRequestSize) {
        if (!this.maxRequestSizeSet) {
            if (maxRequestSize > 0) {
                this.maxRequestSize = maxRequestSize;
                this.maxRequestSizeSet = true;
            } else {
                throw new IllegalArgumentException("'maxRequestSize' must be > 0");
            }
        } else {
            throw new IllegalArgumentException("'maxRequestSize' can only be set once per instance");
        }
    }

    private void assertBytesValid(byte[] bytes) {
        if (bytes != null) {
            if (bytes.length == 0) {
                throw new IllegalArgumentException("'bytes' must not be empty");
            }
        }
    }

    private void validateInput(InputStream contentStream, String topic, int lastAckedMessageIndex) {
        if (contentStream == null) {
            throw new IllegalArgumentException("'contentStream' must not be null");
        } else if (topic == null || topic.trim().length() == 0) {
            throw new IllegalArgumentException("'topic' must not be null or empty");
        } else if (lastAckedMessageIndex < -1) {
            throw new IllegalArgumentException("'lastAckedMessageIndex' must be >= -1");
        }
    }
}
