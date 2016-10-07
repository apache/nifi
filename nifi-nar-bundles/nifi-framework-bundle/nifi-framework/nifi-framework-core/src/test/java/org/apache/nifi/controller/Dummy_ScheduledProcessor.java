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
@DefaultSchedule(ConcurrentTasks = 5, Strategy = SchedulingStrategy.CRON_DRIVEN,Period = "0 0 0 1/1 * ?")
public class Dummy_ScheduledProcessor extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
