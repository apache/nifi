package org.apache.nifi.processors.aws.s3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@TriggerWhenEmpty
@TriggerSerially
@Tags({"Amazon", "S3", "AWS", "Get", "List"})
@CapabilityDescription("Retrieves a listing of objects from S3. For each object that is listed in S3, creates a FlowFile that represents "
        + "the S3 object so that it can be fetched in conjunction with FetchS3Object. This Processor does not delete any data from S3.")
@SeeAlso({FetchS3Object.class, PutS3Object.class})
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "filename", description = "The key under which the object is stored"),
    @WritesAttribute(attribute = "s3.owner", description = "Owner of the object"),
    @WritesAttribute(attribute = "s3.lastModified", description = "Date (millis) when object was last modified"),
    @WritesAttribute(attribute = "s3.length", description = "Object length"),
    @WritesAttribute(attribute = "s3.storageClass", description = "Object storage class"),    
})
public class ListS3 extends AbstractS3Processor {
    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
        .name("Object Key Prefix")
        .description("Optional parameter restricting the response to keys which begin with the specified prefix.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAX_OBJECTS = new PropertyDescriptor.Builder()
        .name("Max Objects")
        .description("Maximum number of objects to output.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
        .name("Minimum Age")
        .description("The minimum age that an object must be in order to be included in the listing; any object younger than this amount of time (based on last modification date) will be ignored")
        .required(true)
        .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
        .name("Maximum Age")
        .description("The maximum age that a object must be in order to be included in the listing; any object older than this amount of time (based on last modification date) will be ignored")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();
    
    private int maxObjects = 0;
    private long minAge = 0L, maxAge = 0L;
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(BUCKET);
        props.add(PREFIX);
        props.add(REGION);
        props.add(ACCESS_KEY);
        props.add(SECRET_KEY);
        props.add(CREDENTIALS_FILE);
        props.add(TIMEOUT);
        props.add(MAX_OBJECTS);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        return Collections.unmodifiableList(props);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return Collections.unmodifiableSet(relationships);
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minAge = (minAgeProp == null) ? 0L : minAgeProp;
        final long maxAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
        if (minAge > maxAge) {
            problems.add(new ValidationResult.Builder().valid(false).subject(getClass().getSimpleName() + " Configuration")
                    .explanation(MIN_AGE.getName() + " cannot be greater than " + MAX_AGE.getName()).build());
        }

        return problems;
    }
    
    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        
        final Integer maxObjectsProp = context.getProperty(MAX_OBJECTS).asInteger();
        maxObjects = (maxObjectsProp == null) ? Integer.MAX_VALUE : maxObjectsProp;
        
        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        minAge = (minAgeProp == null) ? 0L : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AmazonS3 client = getClient();
        
        // build request
        final ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(context.getProperty(BUCKET).evaluateAttributeExpressions().getValue());
        if (context.getProperty(PREFIX).isSet()) {
            request.setPrefix(context.getProperty(PREFIX).evaluateAttributeExpressions().getValue());
        }
        
        // get listing
        ObjectListing listing = client.listObjects(request);
        
        int objectCount = 0;
        // process first batch
        objectCount = processObjects(listing, objectCount, session);
        
        // retrieve and process any remaining batches
        while (listing.isTruncated() && objectCount < maxObjects) {
            listing = client.listNextBatchOfObjects(listing);
            objectCount = processObjects(listing, objectCount, session);
        }
        
        if ( objectCount > 0 ) {
            String listingBase = request.getBucketName() + '/' + (request.getPrefix() != null ? request.getPrefix() : "");
            getLogger().info("Successfully created S3 listing for {} with {} objects",
                    new Object[] {listingBase, objectCount});
            session.commit();
            
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
            return;
        }
    }
    
    private int processObjects(final ObjectListing listing, int objectCount, final ProcessSession session) {
        if (listing.getObjectSummaries() != null) {
            for (S3ObjectSummary s3ObjectSummary : listing.getObjectSummaries()) {
                if (filterByLastModified(s3ObjectSummary)) {
                    final Map<String, String> attributes = createAttributes(s3ObjectSummary);
                    FlowFile flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.transfer(flowFile, REL_SUCCESS);
                    
                    if (++objectCount >= maxObjects) {
                        break;
                    }
                }
            }
        }
        
        return objectCount;
    }
    
    private boolean filterByLastModified(final S3ObjectSummary s3ObjectSummary) {
        final long fileAge = System.currentTimeMillis() - s3ObjectSummary.getLastModified().getTime();
        return (minAge < fileAge && fileAge < maxAge);
    }
    
    private Map<String, String> createAttributes(final S3ObjectSummary s3ObjectSummary) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("s3.bucket", s3ObjectSummary.getBucketName());
        attributes.put(CoreAttributes.FILENAME.key(), s3ObjectSummary.getKey());

        final Owner owner = s3ObjectSummary.getOwner();
        attributes.put("s3.owner", owner != null ? owner.getDisplayName() : "");
        attributes.put("s3.lastModified", String.valueOf(s3ObjectSummary.getLastModified().getTime()));
        attributes.put("s3.length", String.valueOf(s3ObjectSummary.getSize()));
        attributes.put("s3.storageClass", s3ObjectSummary.getStorageClass());

        return attributes;
    }
}
