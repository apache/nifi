package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.AbstractElasticSearchProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.*;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.google.gson.*;

@EventDriven
@Tags({"elasticsearch", "insert", "update", "write", "put"})
@CapabilityDescription("Writes the contents of a FlowFile to ElasticSearch")
public class PutElasticSearch extends AbstractElasticSearchProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to ElasticSearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to ElasticSearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    private final List<PropertyDescriptor> descriptors;

    private final Set<Relationship> relationships;

    public PutElasticSearch() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(BATCH_SIZE);
        descriptors.add(INDEX_STRATEGY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();

        try {
            final BulkRequestBuilder bulk = GetEsBulkRequest(session, flowFiles);
            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    final FlowFile flowFile = flowFiles.get(item.getItemId());
                    if (item.isFailed()) {
                        logger.error("Failed to insert {} into ElasticSearch due to {}",
                                new Object[]{flowFile, item.getFailure()}, new Exception());
                        session.transfer(flowFile, REL_FAILURE);

                    } else {
                        session.transfer(flowFile, REL_SUCCESS);

                    }
                }
            } else {
                for (final FlowFile flowFile : flowFiles) {
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }


        } catch (NoNodeAvailableException nne) {
            logger.error("Failed to insert {} into ElasticSearch No Node Available {}", new Object[]{nne}, nne);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ElasticsearchTimeoutException ete) {
            logger.error("Failed to insert {} into ElasticSearch Timeout to {}", new Object[]{ete}, ete);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ReceiveTimeoutTransportException rtt) {
            logger.error("Failed to insert {} into ElasticSearch ReceiveTimeoutTransportException to {}", new Object[]{rtt}, rtt);
            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (ElasticsearchParseException esp) {
            logger.error("Failed to insert {} into ElasticSearch Parse Exception {}", new Object[]{esp}, esp);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();

        } catch (ElasticsearchException e) {
            logger.error("Failed to insert {} into ElasticSearch due to {}", new Object[]{e}, e);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to insert {} into ElasticSearch due to {}", new Object[]{e}, e);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();

        }
    }

    /**
     * Get the ES bulk request for the session
     *
     * @param session   ProcessSession
     * @param flowFiles Flowfiles pulled off of the queue to batch in
     * @return BulkeRequestBuilder
     */
    private BulkRequestBuilder GetEsBulkRequest(final ProcessSession session, final List<FlowFile> flowFiles) {

        final BulkRequestBuilder bulk = esClient.prepareBulk();
        for (FlowFile file : flowFiles) {
            final byte[] content = new byte[(int) file.getSize()];
            session.read(file, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);

                    final String input = new String(content);
                    final JsonParser parser = new JsonParser();
                    final JsonObject json = parser.parse(input).getAsJsonObject();
                    bulk.add(esClient.prepareIndex(getIndex(json), getType(json), getId(json))
                            .setSource(getSource(json)));

                }

            });

        }
        return bulk;
    }
}
