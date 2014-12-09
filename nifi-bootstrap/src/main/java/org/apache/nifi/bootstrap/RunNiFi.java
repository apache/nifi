package org.apache.nifi.bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Bootstrap class to run Apache NiFi.
 * 
 * This class looks for the bootstrap.conf file by looking in the following places (in order):
 * <ol>
 * 	<li>First argument to the program</li>
 *  <li>Java System Property named {@code org.apache.nifi.bootstrap.config.file}</li>
 *  <li>${NIFI_HOME}/./conf/bootstrap.conf, where ${NIFI_HOME} references an environment variable {@code NIFI_HOME}</li>
 *  <li>./conf/bootstrap.conf, where {@code .} represents the working directory.
 * </ol>
 *
 * If the {@code bootstrap.conf} file cannot be found, throws a {@code FileNotFoundException].
 */
public class RunNiFi {
	public static final String DEFAULT_CONFIG_FILE = "./conf/boostrap.conf";
	public static final String DEFAULT_NIFI_PROPS_FILE = "./conf/nifi.properties";
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(final String[] args) throws IOException, InterruptedException {
		final ProcessBuilder builder = new ProcessBuilder();

		String configFilename = (args.length > 0) ? args[0] : System.getProperty("org.apache.nifi.boostrap.config.file");
		
		if ( configFilename == null ) {
			final String nifiHome = System.getenv("NIFI_HOME");
			if ( nifiHome != null ) {
				final File nifiHomeFile = new File(nifiHome.trim());
				final File configFile = new File(nifiHomeFile, DEFAULT_CONFIG_FILE);
				configFilename = configFile.getAbsolutePath();
			}
		}
		
		if ( configFilename == null ) {
			configFilename = DEFAULT_CONFIG_FILE;
		}
		
		final File configFile = new File(configFilename);
		if ( !configFile.exists() ) {
			throw new FileNotFoundException(DEFAULT_CONFIG_FILE);
		}
		
		final Properties properties = new Properties();
		try (final FileInputStream fis = new FileInputStream(configFile)) {
			properties.load(fis);
		}
		
		final Map<String, String> props = new HashMap<>();
		props.putAll( (Map) properties );

		final String specifiedWorkingDir = props.get("working.dir");
		if ( specifiedWorkingDir != null ) {
			builder.directory(new File(specifiedWorkingDir));
		}

		final File workingDir = builder.directory();
		
		final String libFilename = replaceNull(props.get("lib.dir"), "./lib").trim();
		File libDir = getFile(libFilename, workingDir);
		
		final String confFilename = replaceNull(props.get("conf.dir"), "./conf").trim();
		File confDir = getFile(confFilename, workingDir);
		
		String nifiPropsFilename = props.get("props.file");
		if ( nifiPropsFilename == null ) {
			if ( confDir.exists() ) {
				nifiPropsFilename = new File(confDir, "nifi.properties").getAbsolutePath();
			} else {
				nifiPropsFilename = DEFAULT_CONFIG_FILE;
			}
		}
		
		nifiPropsFilename = nifiPropsFilename.trim();
		
		final List<String> javaAdditionalArgs = new ArrayList<>();
		for ( final Map.Entry<String, String> entry : props.entrySet() ) {
			final String key = entry.getKey();
			final String value = entry.getValue();
			
			if ( key.startsWith("java.arg") ) {
				javaAdditionalArgs.add(value);
			}
		}
		
		final File[] libFiles = libDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String filename) {
				return filename.toLowerCase().endsWith(".jar");
			}
		});
		
		if ( libFiles == null || libFiles.length == 0 ) {
			throw new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath());
		}
		
		final File[] confFiles = confDir.listFiles();
		if ( confFiles == null || confFiles.length == 0 ) {
			throw new RuntimeException("Could not find conf directory at " + confDir.getAbsolutePath());
		}

		final Path workingDirPath = workingDir.toPath();
		final List<String> cpFiles = new ArrayList<>(confFiles.length + libFiles.length);
		cpFiles.add(confDir.getAbsolutePath());
		for ( final File file : libFiles ) {
			final Path path = workingDirPath.relativize(file.toPath());
			final String cpPath = path.toString();
			cpFiles.add(cpPath);
		}
		
		final StringBuilder classPathBuilder = new StringBuilder();
		for (int i=0; i < cpFiles.size(); i++) {
			final String filename = cpFiles.get(i);
			classPathBuilder.append(filename);
			if ( i < cpFiles.size() - 1 ) {
				classPathBuilder.append(File.pathSeparatorChar);
			}
		}

		final String classPath = classPathBuilder.toString();
		String javaCmd = props.get("java");
		if ( javaCmd == null ) {
			javaCmd = "java";
		}
		
		final List<String> cmd = new ArrayList<>();
		cmd.add(javaCmd);
		cmd.add("-classpath");
		cmd.add(classPath);
		cmd.addAll(javaAdditionalArgs);
		cmd.add("-Dnifi.properties.file.path=" + nifiPropsFilename);
		cmd.add("org.apache.nifi.NiFi");
		
		builder.command(cmd).inheritIO();
		
		final StringBuilder cmdBuilder = new StringBuilder();
		for ( final String s : cmd ) {
			cmdBuilder.append(s).append(" ");
		}
		System.out.println("Starting Apache NiFi...");
		System.out.println("Working Directory: " + workingDir.getAbsolutePath());
		System.out.println("Command: " + cmdBuilder.toString());

		final Process proc = builder.start();
		Runtime.getRuntime().addShutdownHook(new ShutdownHook(proc));
		final int statusCode = proc.waitFor();
		System.out.println("Apache NiFi exited with Status Code " + statusCode);
	}
	
	
	private static File getFile(final String filename, final File workingDir) {
		File libDir = new File(filename);
		if ( !libDir.isAbsolute() ) {
			libDir = new File(workingDir, filename);
		}
		
		return libDir;
	}
	
	private static String replaceNull(final String value, final String replacement) {
		return (value == null) ? replacement : value;
	}
}
