package org.apache.nifi.controller.status.history;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComponentStatusHistory {

    private final MetricRollingBuffer snapshots;
    private ComponentDetails componentDetails;

    public ComponentStatusHistory(final ComponentDetails details, final int maxCapacity) {
        this.componentDetails = details;
        snapshots = new MetricRollingBuffer(maxCapacity);
    }

    public void expireBefore(final Date timestamp) {
        snapshots.expireBefore(timestamp);
    }

    public void update(final StatusSnapshot snapshot, final ComponentDetails details) {
        if (snapshot == null) {
            return;
        }

        snapshots.update(snapshot);
        componentDetails = details;
    }

    public StatusHistory toStatusHistory(final List<Date> timestamps, final boolean includeCounters, final Set<MetricDescriptor<?>> defaultStatusMetrics) {
        final Date dateGenerated = new Date();
        final Map<String, String> componentDetailsMap = componentDetails.toMap();
        final List<StatusSnapshot> snapshotList = snapshots.getSnapshots(timestamps, includeCounters, defaultStatusMetrics);
        return new StandardStatusHistory(snapshotList, componentDetailsMap, dateGenerated);
    }
}
