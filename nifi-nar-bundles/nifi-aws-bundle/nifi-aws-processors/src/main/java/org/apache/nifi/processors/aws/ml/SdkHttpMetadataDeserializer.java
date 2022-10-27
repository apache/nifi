package org.apache.nifi.processors.aws.ml;

import com.amazonaws.http.SdkHttpMetadata;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import java.io.IOException;

public class SdkHttpMetadataDeserializer extends StdNodeBasedDeserializer<SdkHttpMetadata> {

    protected SdkHttpMetadataDeserializer() {
        super(SdkHttpMetadata.class);
    }

    @Override
    public SdkHttpMetadata convert(JsonNode root, DeserializationContext ctxt) throws IOException {
        return null;
    }
}
