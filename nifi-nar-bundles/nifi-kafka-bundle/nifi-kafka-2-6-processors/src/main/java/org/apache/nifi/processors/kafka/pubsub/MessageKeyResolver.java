package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.serialization.record.Record;

@FunctionalInterface
interface MessageKeyResolver {
    String apply(FlowFile flowFile, Record record, PropertyValue messageKeyField);
}
