package org.apache.nifi.processors.adx.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.nifi.adx.AdxConnectionService;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.sink.enums.AzureAdxIngestProcessorParamsEnum;
import org.apache.nifi.processors.adx.sink.enums.RelationshipStatusEnum;
import org.apache.nifi.processors.adx.source.enums.AzureAdxSourceProcessorParamsEnum;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
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

    private AdxConnectionService service;

    private Set<Relationship> relationships;

    private List<PropertyDescriptor> descriptors;
    private Client executionClient;

    private ObjectMapper objectMapper = new ObjectMapper();

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

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("kusto-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_QUERY = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.name())
            .displayName(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.getParamDisplayName())
            .description(AzureAdxSourceProcessorParamsEnum.ADX_QUERY.getParamDescription())
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(AdxConnectionService.class)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ADX_SERVICE);
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
        service = context.getProperty(ADX_SERVICE).asControllerService(AdxConnectionService.class);
        executionClient = service.getKustoExecutionClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile outgoingFlowFile = session.create();
        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(outgoingFlowFile).getValue());

        String databaseName = context.getProperty(DB_NAME).getValue();
        String adxQuery = context.getProperty(ADX_QUERY).getValue();
        KustoResultSetTable kustoResultSetTable;

        try {
            //execute Query
            kustoResultSetTable = executionClient.execute(databaseName,adxQuery).getPrimaryResults();
            List<List<Object>> tableData = kustoResultSetTable.getData();

            String json = tableData.size() == 1 ? objectMapper.writeValueAsString(tableData.get(0)) : objectMapper.writeValueAsString(tableData);

            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", new Object[] {tableData});
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(charset));
            session.importFrom(bais, outgoingFlowFile);
            bais.close();

            //if no error then
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, ADX_EXECUTED_QUERY, String.valueOf(adxQuery));
            session.transfer(outgoingFlowFile, RL_SUCCEEDED);
        } catch (DataServiceException | DataClientException | IOException e) {
            getLogger().error("Exception occured while reading data from ADX ", e);
            session.transfer(outgoingFlowFile, RL_FAILED);
        }

    }



}
