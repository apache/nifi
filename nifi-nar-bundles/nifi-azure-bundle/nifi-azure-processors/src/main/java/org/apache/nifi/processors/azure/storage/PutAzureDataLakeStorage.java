package org.apache.nifi.processors.azure.storage;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.apache.nifi.stream.io.StreamUtils;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

@Tags({ "azure", "microsoft", "cloud", "storage", "data lake storage" })
@SeeAlso({ ListAzureBlobStorage.class, FetchAzureBlobStorage.class, DeleteAzureBlobStorage.class })
@CapabilityDescription("Puts content into an Azure data lake storage")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({ @WritesAttribute(attribute = "azure.datalake.fqdn", description = "Fully qualified domain name of the Azure Data Lake Storage account"),
        @WritesAttribute(attribute = "azure.datalake.file", description = "File name with extension copied in Azure Data Lake Storage "),
        @WritesAttribute(attribute = "azure.datalake.file.path", description = "Folder path in Data Lake Storage where file is copied")})
public class PutAzureDataLakeStorage extends AbstractProcessor {

	private static AccessTokenProvider provider = null;
	private static ADLStoreClient client = null;
	
	final static String AZURE_DATALAEK_FQDN = "azure.datalake.fqdn";
    final static String AZURE_DATALAEK_FILE = "azure.datalake.file";
    final static String AZURE_DATALAEK_FILE_PATH = "azure.datalake.file.path";
    

	public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder().name("clientid")
			.displayName("Client Id")
			.description(
					"ClientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder().name("clientsecret")
			.displayName("Client Secret").description("ClientSecret the secret key of the Azure client web app")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final PropertyDescriptor AUTH_ENDPOINT = new PropertyDescriptor.Builder().name("authendpoint")
			.displayName("Auth EndPoint")
			.description(
					"OAuth 2.0 token endpoint associated with the user's directory (obtain from Active Directory configuration)")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final PropertyDescriptor ACCOUNT_FQDN = new PropertyDescriptor.Builder().name("accountfqdn")
			.displayName("Account FQDN")
			.description(
					"Fully qualified domain name of the account.For example, <<datalakestroageaccount>>.azuredatalakestore.net")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final PropertyDescriptor FOLDER_PATH = new PropertyDescriptor.Builder().name("path")
			.displayName("Path").description("Folder path in Data Lake Storage where file will be copied ")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder().name("file").displayName("File")
			.description("File name with extension to be copied in Azure Data Lake Storage ")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All successfully processed FlowFiles are routed to this relationship").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Unsuccessful operations will be transferred to the failure relationship.").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(CLIENT_SECRET);
		descriptors.add(CLIENT_ID);
		descriptors.add(AUTH_ENDPOINT);
		descriptors.add(ACCOUNT_FQDN);
		descriptors.add(FOLDER_PATH);
		descriptors.add(FILE_NAME);

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
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
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		
		try {
			
			final String clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions(flowFile).getValue();
			final String clientSecret = context.getProperty(CLIENT_SECRET).evaluateAttributeExpressions(flowFile).getValue();
			final String authEndPoint = context.getProperty(AUTH_ENDPOINT).evaluateAttributeExpressions(flowFile).getValue();
			final String accountFQDN = context.getProperty(ACCOUNT_FQDN).evaluateAttributeExpressions(flowFile).getValue();
			final String fileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
			final String folderPath = context.getProperty(FOLDER_PATH).evaluateAttributeExpressions(flowFile).getValue();
			
			final Map<String, String> attributes = new HashMap<>();
			attributes.put(AZURE_DATALAEK_FILE,fileName);
			attributes.put(AZURE_DATALAEK_FILE_PATH,folderPath);
			attributes.put(AZURE_DATALAEK_FQDN,accountFQDN);
			flowFile = session.putAllAttributes(flowFile, attributes);
			
			final byte[] buffer = new byte[(int) flowFile.getSize()];
	        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
	        
	        if(provider == null) {
	        	 provider = new ClientCredsTokenProvider(authEndPoint, clientId, clientSecret);
	 	         client = ADLStoreClient.createClient(accountFQDN, provider);
	        }
	       
			OutputStream stream = client.createFile(folderPath+File.separator+fileName, IfExists.OVERWRITE  );
	        PrintStream out = new PrintStream(stream);
	        out.write(buffer);
	        out.flush();
	        out.close();
	        session.transfer(flowFile, REL_SUCCESS);
		}catch(IOException e){
			getLogger().error("Failed to put file on Data Lake Storage");
			flowFile = session.penalize(flowFile);
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
		

		
	}

}
