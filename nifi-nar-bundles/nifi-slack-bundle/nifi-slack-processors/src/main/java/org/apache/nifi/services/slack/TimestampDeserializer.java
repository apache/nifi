package org.apache.nifi.services.slack;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Date;

public class TimestampDeserializer extends JsonDeserializer<Date> {
    @Override
    public Date deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        final String timestampString = jp.getText().trim();
        final Double milliseconds = Double.valueOf(timestampString) * 1000;

        final Date timestamp = new Date();
        timestamp.setTime(milliseconds.longValue());

        System.out.println(timestamp.getTime());

        return timestamp;
    }
}
