package org.apache.nifi.controller;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;


/**
 * Dummy processor to test @DefaultSchedule annotation
 */
@DefaultSchedule(concurrentTasks = 5, strategy = SchedulingStrategy.CRON_DRIVEN, period = "0 0 0 1/1 * ?")
public class DummyScheduledProcessor extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
