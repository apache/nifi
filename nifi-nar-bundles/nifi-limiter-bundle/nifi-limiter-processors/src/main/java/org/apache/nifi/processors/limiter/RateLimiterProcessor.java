package org.apache.nifi.processors.limiter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public interface RateLimiterProcessor {
    String ATTR_RECORD_COUNT = "record.count";
    String ATTR_RECORD_COUNT_FLOODING = "record.flooding";
    String ATTR_MIME_TYPE = "mime.type";

    static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
        .name("put-key")
        .displayName("Key")
        .description("The key use for each payload to check the rate limit."
         + "This is a replacement value if the record path for the key isn't "
         + "defined or doesn't exist. To check internall cache a key must be"
         + "provided.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    static final PropertyDescriptor REFILL_TOKENS = new PropertyDescriptor.Builder()
        .name("put-refill-tokens")
        .displayName("Refill tokens")
        .description("The refill does refill of tokens in intervally manner.It will wait "
         + "until whole period will be elapsed before regenerate tokens. This parameter "
         + "define the amount of tokens.")
        .required(true)
        .defaultValue("2000")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor REFILL_PERIOD = new PropertyDescriptor.Builder()
        .name("put-refill-period")
        .displayName("Refill period")
        .description("Define the period within tokens will be fully regenerated.")
        .required(true)
        .defaultValue("10 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();

    static final PropertyDescriptor BANDWIDTH_CAPACITY = new PropertyDescriptor.Builder()
        .name("put-bandwidth-capacity")
        .displayName("Bandwidth capacity")
        .description("The bandwidth is key building block for bucket. The bandwidth "
         + "consists from capacity and refill. The capacity - defines the maximum count"
         + "of tokens which can be hold by bucket.")
        .required(true)
        .defaultValue("60000")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor MAX_SIZE_BUCKET = new PropertyDescriptor.Builder()
        .name("put-max-size-bucket")
        .displayName("Max Size Bucket")
        .description("Specifies the maximum number of entries the cache may contain. "
         + "Note that the cache may evict an entry before this limit is exceeded. As "
         + "the cache size grows close to the maximum, the cache evicts entries that "
         + "are less likely to be used again. For example, the cache may evict an "
         + "entry because it hasn't been used recently or very often.")
        .required(true)
        .defaultValue("1000")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor EXPIRE_DURATION = new PropertyDescriptor.Builder()
        .name("put-expire-duration")
        .displayName("Expire After Write Duration")
        .description("Specifies that each entry should be automatically removed from "
         + "the cache once a fixed duration has elapsed after the entry's creation, or"
         + "the most recent replacement of its value.")
        .required(true)
        .defaultValue("60 min")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All records that succeed in being transferred into Elasticsearch go here.")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All records that fail for reasons related to bucket and cache.")
        .build();

    static final Relationship REL_FLOODING = new Relationship.Builder()
        .name("flooding")
        .description("All records flooding the system cached by the rate limiter.")
        .build();


}
