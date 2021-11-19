package org.apache.nifi.processors.limiter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.github.bucket4j.*;

import com.google.common.cache.*;

@Tags({ "record", "rate limiter", "flooding" })
@CapabilityDescription("Provide a system to limit the incoming traffic in nifi. Each flow will be limited using the provided key")
@SeeAlso({ RateMultipleLimiterRecord.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type indicated by the Record Writer") })
public class RateLimiter extends AbstractProcessor implements RateLimiterProcessor {

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(KEY, REFILL_TOKENS, REFILL_PERIOD, BANDWIDTH_CAPACITY, MAX_SIZE_BUCKET, EXPIRE_DURATION));

    static final Set<Relationship> RELATIONSHIPS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_FLOODING)));

    private LoadingCache<String, Bucket> internalCache = null;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            this.internalCache = CacheBuilder.newBuilder()
                    .maximumSize(Integer.parseInt(context.getProperty(MAX_SIZE_BUCKET).getValue()))
                    .expireAfterWrite(
                            context.getProperty(EXPIRE_DURATION).asTimePeriod(TimeUnit.MILLISECONDS).intValue(),
                            TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, Bucket>() {
                        public Bucket load(String key) {
                            Refill refill = Refill.intervally(
                                    Integer.parseInt(context.getProperty(REFILL_TOKENS).getValue()),
                                    Duration.ofMillis(context.getProperty(REFILL_PERIOD)
                                            .asTimePeriod(TimeUnit.MILLISECONDS).intValue()));
                            Bandwidth limit = Bandwidth.classic(
                                    Integer.parseInt(context.getProperty(BANDWIDTH_CAPACITY).getValue()), refill);
                            return Bucket4j.builder().addLimit(limit).build();
                        }
                    });
        } catch (Exception e) {
            getLogger().error("Could not intiate InternalCache", e);
        }
    }

    @OnUnscheduled
    public void onUnScheduled(final ProcessContext context) {
        if (this.internalCache != null) {
            this.internalCache.cleanUp();
            this.internalCache = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String key = context.getProperty(KEY).evaluateAttributeExpressions(input).getValue();

        Bucket mybucket;
        try {
            mybucket = this.internalCache.get(key);

            if (mybucket != null) {
                // try to consume
                ConsumptionProbe probe = mybucket.tryConsumeAndReturnRemaining(1);

                if (probe.isConsumed()) {
                    // RateLimiterRes: "OK"
                    session.transfer(input, REL_SUCCESS);
                } else {
                    // RateLimiterRes: "KO"
                    session.transfer(input, REL_FLOODING);
                }
            } else {
                // RateLimiterRes: "Mybucket Null"
                getLogger().error("Rate Limiter : Mybucket Null");
                session.transfer(input, REL_FAILURE);
            }
        } catch (ExecutionException ex) {
            getLogger().error("Could not check internal cache.", ex);
            session.transfer(input, REL_FAILURE);
            return;
        }

    }

}
