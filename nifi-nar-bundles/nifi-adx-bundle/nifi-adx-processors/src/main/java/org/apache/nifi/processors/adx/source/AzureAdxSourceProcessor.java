package org.apache.nifi.processors.adx;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("This Processor acts as a ADX source connector which queries data from Azure Data Explorer." +
        "The data from Azure Data Explorer can be read through 2 modes Single and Distributed. Single mode is used to fetch data of smaller size." +
        "When the records count exceeds 500000 records the mode is switched back to distributed mode." +
        "Distributed mode exports data into Azure Blob storage asynchronously and writes back into the nifi flowfile. " +
        "This mode should be selected for queries with larger data size. ")
@ReadsAttributes({
        @ReadsAttribute(attribute = "ADX_QUERY", description = "This attribute specifies the source query which needs to be queried in ADX for the relevant data."),
        @ReadsAttribute(attribute = "READ_MODE", description = "Specifies the mode in which the data will be fetched from ADX. Single Mode to used for queries returning small amount of data and Distributed Mode to be used for larger size of data."),
})
public class AzureAdxSourceProcessor extends AbstractProcessor {


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
