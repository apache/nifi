package org.apache.nifi.processors.adx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.nifi.adx.AdxSourceConnectionService;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.enums.RelationshipStatusEnum;
import org.apache.nifi.processors.adx.enums.AzureAdxSourceProcessorParamsEnum;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("This Processor acts as a ADX source connector which queries data from Azure Data Explorer."+
        "This connector can act only as a start of the data pipeline getting data from ADX."+
        "more regarding the kusto query limits can be found here https://learn.microsoft.com/en-us/azure/data-explorer/kusto/concepts/querylimits")
@ReadsAttributes({
        @ReadsAttribute(attribute = "DB_NAME", description = "This attribute specifies the database in ADX cluster where the query needs to be executed."),
        @ReadsAttribute(attribute = "ADX_QUERY", description = "This attribute specifies the source query which needs to be queried in ADX for the relevant data.")
})
public class AzureAdxSourceProcessor extends AbstractProcessor {

    private AdxSourceConnectionService service;

    private Set<Relationship> relationships;

    private List<PropertyDescriptor> descriptors;
    private Client executionClient;

    private ObjectMapper objectMapper = new ObjectMapper();

    public static final String ADX_QUERY_ERROR_MESSAGE = "adx.query.error.message";

    public static final String ADX_EXECUTED_QUERY = "adx.executed.query";

    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_SUCCEEDED.name())
            .description(RelationshipStatusEnum.RL_SUCCEEDED.getDescription())
            .build();
    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_FAILED.name())
            .description(RelationshipStatusEnum.RL_FAILED.getDescription())
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParamsEnum.DB_NAME.name())
            .displayName(AzureAdxSourceProcessorParamsEnum.DB_NAME.getParamDisplayName())
            .description(AzureAdxSourceProcessorParamsEnum.DB_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_QUERY = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.name())
            .displayName(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.getParamDisplayName())
            .description(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_SOURCE_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParamsEnum.ADX_SOURCE_SERVICE.name())
            .displayName(AzureAdxSourceProcessorParamsEnum.ADX_SOURCE_SERVICE.getParamDisplayName())
            .description(AzureAdxSourceProcessorParamsEnum.ADX_SOURCE_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(AdxSourceConnectionService.class)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ADX_SOURCE_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(ADX_QUERY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RL_SUCCEEDED);
        relationships.add(RL_FAILED);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(ADX_SOURCE_SERVICE).asControllerService(AdxSourceConnectionService.class);
        executionClient = service.getKustoExecutionClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile outgoingFlowFile;

        String databaseName = context.getProperty(DB_NAME).getValue();
        String adxQuery = null;
        KustoResultSetTable kustoResultSetTable;

        if ( context.hasIncomingConnection() ) {
            FlowFile incomingFlowFile = session.get();

            if ( incomingFlowFile == null && context.hasNonLoopConnection() ) {
                return;
            }

            if ( incomingFlowFile.getSize() == 0 ) {
                if ( context.getProperty(ADX_QUERY).isSet() ) {
                    adxQuery = context.getProperty(ADX_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
                } else {
                    String message = "FlowFile query is empty and no scheduled query is set";
                    getLogger().error(message);
                    incomingFlowFile = session.putAttribute(incomingFlowFile, ADX_QUERY_ERROR_MESSAGE, message);
                    session.transfer(incomingFlowFile, RL_FAILED);
                    return;
                }
            } else {
                try {
                    adxQuery = getQuery(session, StandardCharsets.UTF_8, incomingFlowFile);
                } catch(IOException ioe) {
                    getLogger().error("Exception while reading from FlowFile " + ioe.getLocalizedMessage(), ioe);
                    throw new ProcessException(ioe);
                }
            }
            outgoingFlowFile = incomingFlowFile;

        } else {
            outgoingFlowFile = session.create();
            adxQuery = context.getProperty(ADX_QUERY).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        }

        try {
            //execute Query
            kustoResultSetTable = executeQuery(databaseName,adxQuery);
            List<List<Object>> tableData = kustoResultSetTable.getData();

            String json = tableData.size() == 1 ? objectMapper.writeValueAsString(tableData.get(0)) : objectMapper.writeValueAsString(tableData);

            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", new Object[] {tableData});
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes());
            session.importFrom(bais, outgoingFlowFile);
            bais.close();

            //if no error then
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, ADX_EXECUTED_QUERY, String.valueOf(adxQuery));
            session.transfer(outgoingFlowFile, RL_SUCCEEDED);
        }catch (OutOfMemoryError ex){
            getLogger().error("Exception occurred while reading data from ADX : Query Limits exceeded : Please modify your query to fetch results below the kusto query limits ", ex);
            session.transfer(outgoingFlowFile, RL_FAILED);
        } catch (DataServiceException | DataClientException | IOException e) {
            getLogger().error("Exception occurred while reading data from ADX ", e);
            session.transfer(outgoingFlowFile, RL_FAILED);
        }

    }

    protected KustoResultSetTable executeQuery(String databaseName, String adxQuery) throws DataServiceException, DataClientException, OutOfMemoryError {
        return executionClient.execute(databaseName,adxQuery).getPrimaryResults();
    }

    protected String getQuery(final ProcessSession session, Charset charset, FlowFile incomingFlowFile)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(incomingFlowFile, baos);
        baos.close();
        return new String(baos.toByteArray(), charset);
    }



}
