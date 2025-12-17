package org.apache.nifi.prometheusutil;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;

public class VersionInfoRegistry extends AbstractMetricsRegistry {

    private static final String DEFAULT_LABEL_STRING = "unknown";

    public VersionInfoRegistry() {
        // Processor / Process Group metrics
        nameToGaugeMap.put("NIFI_VERSION_INFO", Gauge.build()
                .name("nifi_version_info")
                .help("NiFi framework and environment version information.")
                .labelNames("instance_id", "nifi_version", "java_version", "revision", "build_tag",
                        "build_branch", "os_name", "os_version", "os_architecture", "java_vendor")
                .register(registry));
    }

    @Override
    public CollectorRegistry getRegistry() {
        return registry;
    }
    public static class VersionDetails { 
        public final String nifiVersion;
        public final String revision;
        public final String tag;
        public final String buildBranch;
        public final String javaVersion;
        public final String javaVendor;
        public final String osVersion;
        public final String osName;
        public final String osArchitecture;

        public VersionDetails(String nifiVersion, String revision, String tag, String buildBranch,
                              String javaVersion, String javaVendor, String osVersion, 
                              String osName, String osArchitecture) {
            this.nifiVersion = nifiVersion;
            this.revision = revision;
            this.tag = tag;
            this.buildBranch = buildBranch;
            this.javaVersion = javaVersion;
            this.javaVendor = javaVendor;
            this.osVersion = osVersion;
            this.osName = osName;
            this.osArchitecture = osArchitecture;
        }
    }

    public VersionDetails getVersionDetails() {
        String nifiVersion = DEFAULT_LABEL_STRING;
        String revision = DEFAULT_LABEL_STRING;
        String tag = DEFAULT_LABEL_STRING;
        String buildBranch = DEFAULT_LABEL_STRING;

        // Retrieve universal system properties
        final String javaVersion = System.getProperty("java.version", DEFAULT_LABEL_STRING);
        final String javaVendor = System.getProperty("java.vendor", DEFAULT_LABEL_STRING);
        final String osVersion = System.getProperty("os.version", DEFAULT_LABEL_STRING);
        final String osName = System.getProperty("os.name", DEFAULT_LABEL_STRING);
        final String osArchitecture = System.getProperty("os.arch", DEFAULT_LABEL_STRING);

        try {
            // NiFi internal API to get build specifics (this is the isolated access point)
            final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
            if (frameworkBundle != null) {
                final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
                nifiVersion = frameworkDetails.getCoordinate().getVersion();
                revision = frameworkDetails.getBuildRevision();
                tag = frameworkDetails.getBuildTag();
                buildBranch = frameworkDetails.getBuildBranch();
            }
        } catch (Exception e) {
        }
        return new VersionDetails(nifiVersion, revision, tag, buildBranch, javaVersion, javaVendor, osVersion, osName, osArchitecture);
    }
}