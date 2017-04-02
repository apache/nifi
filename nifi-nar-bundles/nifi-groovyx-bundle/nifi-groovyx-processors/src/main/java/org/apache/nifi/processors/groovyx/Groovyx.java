package org.apache.nifi.processors.groovyx;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.codehaus.groovy.runtime.StackTraceUtils;

import org.apache.nifi.processors.groovyx.sql.OSql;
import org.apache.nifi.processors.groovyx.util.Files;
import org.apache.nifi.processors.groovyx.flow.ProcessSessionWrap;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.sql.Sql;

// CHECKSTYLE:OFF
@SuppressWarnings("PMD")
@EventDriven
@Tags({ "script", "groovy", "groovyx", "extended"})
@CapabilityDescription("Extended Groovy script processor. The script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the script. If the handling is incomplete or incorrect, the session will be rolled back."
        )
@SeeAlso({})
@DynamicProperty(name = "A script engine property to update", value = "The value to set it to", supportsExpressionLanguage = true, description = "Updates a script engine property specified by the Dynamic Property's key with the value "
        + "specified by the Dynamic Property's value. Use `CTL.` to access any controller services.")
public class Groovyx extends AbstractProcessor {

	private static final String PRELOADS = "import org.apache.nifi.components.*;" 
			+ "import org.apache.nifi.flowfile.FlowFile;"
			+ "import org.apache.nifi.processor.*;"
			+ "import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;"
			+ "import org.apache.nifi.processor.exception.*;"
			+ "import org.apache.nifi.processor.io.*;"
			+ "import org.apache.nifi.processor.util.*;"
			+ "import org.apache.nifi.processors.script.*;"
			+ "import org.apache.nifi.logging.ComponentLog;";

	public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder().name("Script File").required(false)
			.description("Path to script file to execute. Only one of Script File or Script Body may be used")
			.addValidator(new StandardValidators.FileExistsValidator(true)).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder().name("Script Body").required(false)
			.description("Body of script to execute. Only one of Script File or Script Body may be used")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false).build();

	public static String[] VALID_BOOLEANS = {"true", "false"};
	public static final PropertyDescriptor REQUIRE_FLOW = new PropertyDescriptor.Builder().name("Requires flow file")
			.description("If `true` then flowFile variable initialized and validated. So developer don't need to do flowFile = session.get(). If `false` the flowFile variable not initialized.")
			.required(true).expressionLanguageSupported(false).allowableValues(VALID_BOOLEANS).defaultValue("false").build();

	public static String[] VALID_FAIL_STRATEGY = {"rollback", "transfer to failure"};
	public static final PropertyDescriptor FAIL_STRATEGY = new PropertyDescriptor.Builder().name("Failure strategy")
			.description("If `transfer to failure` used then all flowFiles received from incoming queues in this session in case of exception will be transferred to `failure` relationship with additional attributes set: ERROR_MESSAGE and ERROR_STACKTRACE.")
			.required(true).expressionLanguageSupported(false).allowableValues(VALID_FAIL_STRATEGY).defaultValue(VALID_FAIL_STRATEGY[0]).build();

	public static final PropertyDescriptor ADD_CLASSPATH = new PropertyDescriptor.Builder().name("Additional classpath").required(false)
			.description("Classpath list separated by semicolon. You can use masks like `*`, `*.jar` in file name. Please avoid using this parameter because of deploy complexity :)")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();

	public static final Relationship REL_SUCCESS =
			new Relationship.Builder().name("success").description("FlowFiles that were successfully processed").build();

	public static final Relationship REL_FAILURE =
			new Relationship.Builder().name("failure").description("FlowFiles that failed to be processed").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	GroovyShell shell = null; //new GroovyShell();
	File scriptFile = null;
	String scriptBody = null;
	Class<Script> compiled = null;
	long scriptLastModified = 0;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SCRIPT_FILE);
		descriptors.add(SCRIPT_BODY);
		descriptors.add(REQUIRE_FLOW);
		descriptors.add(FAIL_STRATEGY);
		descriptors.add(ADD_CLASSPATH);
		this.descriptors = Collections.unmodifiableList(descriptors);

		HashSet<Relationship> relationshipSet = new HashSet<Relationship>();
		relationshipSet.add(REL_SUCCESS);
		relationshipSet.add(REL_FAILURE);
		relationships = Collections.unmodifiableSet(relationshipSet);

	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	private void callScriptStatic(String method, final ProcessContext context)
			throws IllegalAccessException, java.lang.reflect.InvocationTargetException {
		if (compiled != null) {
			Method m = null;
			try {
				m = compiled.getDeclaredMethod(method, ProcessContext.class);
			} catch (NoSuchMethodException e) {}
			if(m == null){
				try {
					m = compiled.getDeclaredMethod(method, Object.class);
				} catch (NoSuchMethodException e) {}
			}
			if (m != null) {
				m.invoke(null, context);
			}
		}
	}

	@OnStopped
	public void onStopped(final ProcessContext context) {
		try {
			callScriptStatic("onStop", context);
		} catch (Throwable t) {
			throw new ProcessException("Failed to finalize groovy script:\n" + t, t);
		}
		//reset compiled and shell on stop
		shell = null;
		compiled = null;
		scriptLastModified = 0;
	}

	/**
	 * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
	 * properties, as well as reloading the script (from file or the "Script Body" property)
	 *
	 * @param context the context in which to perform the setup operations
	 */
	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		GroovyMethods.init();
		String scriptPath = context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue();
		scriptBody = context.getProperty(SCRIPT_BODY).getValue();
		String addClasspath = context.getProperty(ADD_CLASSPATH).evaluateAttributeExpressions().getValue();

		if (scriptBody != null && scriptPath != null) {
			throw new ProcessException("Only one parameter accepted: `" + SCRIPT_BODY.getDisplayName()
					+ "` or `"
					+ SCRIPT_FILE.getDisplayName()
					+ "`");
		}
		if (scriptBody == null && scriptPath == null) {
			throw new ProcessException("At least one parameter required: `" + SCRIPT_BODY.getDisplayName()
					+ "` or `"
					+ SCRIPT_FILE.getDisplayName()
					+ "`");
		}
		try {
			scriptFile = scriptPath == null ? null : new File(scriptPath);
			CompilerConfiguration conf = new CompilerConfiguration();
			conf.setDebug(true);
			shell = new GroovyShell(conf);

			if (addClasspath != null && addClasspath.length() > 0) {
				for (File fcp : Files.listPathsFiles(addClasspath)) {
					shell.getClassLoader().addClasspath(fcp.toString());
				}
			}
			//try to add classpath with groovy classes
			String groovyPath = context.newPropertyValue("${groovy.classes.path}").evaluateAttributeExpressions().getValue();
			if(groovyPath==null || groovyPath.length()==0) {
				groovyPath = context.newPropertyValue("${resources.path}").evaluateAttributeExpressions().getValue();
				if(groovyPath!=null && groovyPath.length()>0) {
					groovyPath = groovyPath+"/common/classes";
				}
			}
			if(groovyPath!=null && groovyPath.length()>0){
				shell.getClassLoader().addClasspath(groovyPath);
			}
			//clear compiled script and compile it again
			compiled = null;
			getGroovyScript();
		} catch (Throwable t) {
			throw new ProcessException("Failed to initialize groovy engine:\n" + t, t);
		}
		try {
			callScriptStatic("onStart", context);
		} catch (Throwable t) {
			throw new ProcessException("Failed to initialize groovy script:\n" + t, t);
		}
	}

	Script getGroovyScript() throws Throwable {
		Script script = null;
		Class _compiled = compiled;
		if (_compiled != null && scriptFile != null
				&& scriptLastModified != scriptFile.lastModified()
				&& System.currentTimeMillis() - scriptFile.lastModified() > 3000) {
			System.out.println("Recompile "+_compiled);
			_compiled = null;
		}
		if (_compiled == null) {
			String scriptName = null;
			String scriptText = null;
			if (scriptFile != null) {
				scriptName = scriptFile.getName();
				scriptLastModified = scriptFile.lastModified();
				scriptText = ResourceGroovyMethods.getText(scriptFile, "UTF-8");
			} else {
				scriptName = "Script" + Long.toHexString(scriptBody.hashCode()) + ".groovy";
				scriptText = scriptBody;
			}
			script = shell.parse(PRELOADS + scriptText, scriptName);
			_compiled = script.getClass();
			compiled = _compiled;
		}
		if (script == null) {
			script = (Script) _compiled.newInstance();
		}
		Thread.currentThread().setContextClassLoader(shell.getClassLoader());
		return script;
	}

	//init
	private void onInitCTL(HashMap CTL)throws SQLException{
		for(Map.Entry e : (Set<Map.Entry>)CTL.entrySet()){
			if(e.getValue() instanceof DBCPService){
				DBCPService s = (DBCPService)e.getValue();
				OSql sql = new OSql(s.getConnection());
				sql.getConnection().setAutoCommit(false);
				e.setValue(sql);
			}
		}
	}

	//before commit
	private void onCommitCTL(HashMap CTL)throws SQLException{
		for(Map.Entry e : (Set<Map.Entry>)CTL.entrySet()){
			if(e.getValue() instanceof OSql){
				OSql sql = (OSql)e.getValue();
				sql.commit();
			}
		}
	}

	//finalize
	private void onFinitCTL(HashMap CTL){
		for(Map.Entry e : (Set<Map.Entry>)CTL.entrySet()){
			if(e.getValue() instanceof OSql){
				OSql sql = (OSql)e.getValue();
				try {
					//sql.commit();
					sql.getConnection().setAutoCommit(true); //default autocommit value in nifi
					sql.close();
					sql = null;
				} catch (Throwable ei) {
				}
			}
		}
	}

	//exception
	private void onFailCTL(HashMap CTL){
		for(Map.Entry e : (Set<Map.Entry>)CTL.entrySet()){
			if(e.getValue() instanceof OSql){
				OSql sql = (OSql)e.getValue();
				try {
					sql.rollback();
				} catch (Throwable ei) {
				}
			}
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession _session) throws ProcessException {
		String requireFlow = context.getProperty(REQUIRE_FLOW).getValue();
		boolean toFailureOnError = VALID_FAIL_STRATEGY[1].equals( context.getProperty(FAIL_STRATEGY).getValue() );
		ProcessSessionWrap session = new ProcessSessionWrap(_session, toFailureOnError);

		FlowFile flowFile = null;
		Sql sql = null;
		boolean autocommit = true;

		if ("true".equals(requireFlow)) {
			flowFile = session.get();
			if (flowFile == null) {
				return;
			}
		}else{
			if(toFailureOnError) {
				throw new ProcessException("The parameter `"+REQUIRE_FLOW.getName()+"` must be true when `"+FAIL_STRATEGY.getName()+"` is "+VALID_FAIL_STRATEGY[1]);
			}
		}

		HashMap CTL = new HashMap(){
			@Override
			public Object get(Object key) {
				if(!containsKey(key)) {
					throw new RuntimeException("The `CTL."+key+"` not defined in processor properties");
				}
				return super.get(key);
			}
		};

		try {
			Script script = getGroovyScript();
			Map bindings = script.getBinding().getVariables();

			bindings.clear();

			// Find the user-added properties and set them on the script
			for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
				if (property.getKey().isDynamic()) {
					if(property.getKey().getName().startsWith("CTL.")){
						//get controller service
						ControllerService ctl = context.getProperty(property.getKey()).asControllerService(ControllerService.class);
						CTL.put( property.getKey().getName().substring(4), ctl );
					}else{
						// Add the dynamic property bound to its full PropertyValue to the script engine
						if (property.getValue() != null) {
							bindings.put(property.getKey().getName(), context.getProperty(property.getKey()));
						}
					}                }
			}
			onInitCTL(CTL);

			bindings.put("session", session);
			bindings.put("context", context);
			bindings.put("log", getLogger());
			bindings.put("REL_SUCCESS", REL_SUCCESS);
			bindings.put("REL_FAILURE", REL_FAILURE);
			bindings.put("CTL", CTL);
			if (flowFile != null) {
				bindings.put("flowFile", flowFile);
			}

			script.run();
			bindings.clear();

			onCommitCTL(CTL);
			session.commit();
		} catch (Throwable t) {
			if (sql != null) {
				try {
					sql.rollback();
				} catch (Throwable ei) {
				}
			}
			onFailCTL(CTL);
			if(toFailureOnError){
				getLogger().error(t.toString(),t);
				session.revertToFailure(REL_FAILURE, StackTraceUtils.deepSanitize(t) );
			}else{
				session.rollback(true);
				throw new ProcessException(t);
			}
		} finally {
			if (sql != null) {
				try {
					if (autocommit != false) {
						sql.getConnection().setAutoCommit(autocommit);
					}
				} catch (Throwable ei) {
				}
				try {
					sql.close();
					sql = null;
				} catch (Throwable ei) {
				}
			}
			onFinitCTL(CTL);
		}

	}

	/**
	 * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
	 * which will be available as variables in the script
	 *
	 * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
	 * @return a PropertyDescriptor object corresponding to the specified dynamic property name
	 */
	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
		if(propertyDescriptorName.startsWith("CTL.")){
			return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false)
				.description("Controller service accessible from code as `"+propertyDescriptorName+"`")
				.dynamic(true).identifiesControllerService(ControllerService.class).build();
		}
		return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).dynamic(true).build();
	}

}
