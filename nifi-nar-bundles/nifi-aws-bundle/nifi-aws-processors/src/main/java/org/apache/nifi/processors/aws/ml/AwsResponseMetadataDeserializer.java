package org.apache.nifi.processors.aws.ml;

import com.amazonaws.ResponseMetadata;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import java.io.IOException;
import java.util.Map;

public class AwsResponseMetadataDeserializer extends StdNodeBasedDeserializer<ResponseMetadata> {
    protected AwsResponseMetadataDeserializer() {
        super(ResponseMetadata.class);
    }

    @Override
    public ResponseMetadata convert(JsonNode root, DeserializationContext ctxt) throws IOException {
        return new ResponseMetadata((Map<String, String>) null);
    }
}
