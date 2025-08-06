package org.apache.nifi.processors.aws.kinesis3;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

import static java.nio.charset.StandardCharsets.UTF_8;

final class KinesisRecordPayload {

    static String extract(final KinesisClientRecord record) {
        record.data().rewind();

        final byte[] buffer = new byte[record.data().remaining()];
        record.data().get(buffer);

        return new String(buffer, UTF_8);
    }

    private KinesisRecordPayload() {
    }
}
