/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.ConfigurableComponentInitializer;
import org.apache.nifi.documentation.html.HtmlDocumentationWriter;
import org.apache.nifi.documentation.html.HtmlProcessorDocumentationWriter;
import org.apache.nifi.documentation.init.ControllerServiceInitializer;
import org.apache.nifi.documentation.init.ProcessorInitializer;
import org.apache.nifi.documentation.init.ReportingTaskingInitializer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;

@Mojo(name = "document", defaultPhase = LifecyclePhase.PROCESS_CLASSES, threadSafe = false, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, requiresProject = true)
public class DocumentMojo extends AbstractMojo {

    /**
     * Location of the file.
     */
    @Parameter(defaultValue = "${project.build.outputDirectory}", property = "outputDir", required = true)
    private File outputDirectory;

    @Parameter(defaultValue = "${project}", required = true)
    private MavenProject mavenProject;

    public void execute() throws MojoExecutionException {
        final File docsDir = new File(outputDirectory, "docs");
        if (!docsDir.exists()) {
            docsDir.mkdirs();
        }

        try {
            final List<?> compileClassPathElements = mavenProject.getCompileClasspathElements();
            // final List<?> compileClassPathElements = mavenProject.getRuntimeClasspathElements();
            final URL[] urls = new URL[compileClassPathElements.size()];

            for (int i = 0; i < compileClassPathElements.size(); i++) {
                final String path = compileClassPathElements.get(i).toString();
                final File filePath = new File(path);
                final URL url = filePath.toURI().toURL();
                urls[i] = url;
                getLog().info("Adding url: " + url);
            }

            final ClassLoader myClassLoader = Thread.currentThread().getContextClassLoader();
            final ClassLoader projectClassLoader = new URLClassLoader(urls, myClassLoader);

            final List<ConfigurableComponent> components = new ArrayList<>();
            loadServices(projectClassLoader, components, Processor.class);
            loadServices(projectClassLoader, components, ControllerService.class);
            loadServices(projectClassLoader, components, ReportingTask.class);

            for (ConfigurableComponent component : components) {
                initializeComponent(component);
                generateDocumentation(component, projectClassLoader, docsDir);
                tearDownComponent(component);
            }

        } catch (DependencyResolutionRequiredException e) {
            throw new MojoExecutionException("Unable to resolve classpath", e);
        } catch (MalformedURLException e) {
            throw new MojoExecutionException("Unable to resolve classpath", e);
        } catch (InitializationException e) {
            throw new MojoExecutionException("Unable to initializer component", e);
        } catch (FileNotFoundException e) {
            throw new MojoExecutionException("Unable to generate documentation", e);
        } catch (IOException e) {
            throw new MojoExecutionException("Unable to generate documentation", e);
        } catch (ServiceConfigurationError e) {
            throw new MojoExecutionException("Unable to generate documentation", e);
        }
    }

    private static <T extends ConfigurableComponent> void loadServices(final ClassLoader projectClassLoader, final List<ConfigurableComponent> components, final Class<T> service) {
        final ServiceLoader<T> nifiServiceLoader = ServiceLoader.load(service, projectClassLoader);

        for (T processor : nifiServiceLoader) {
            components.add(processor);
        }
    }

    private void tearDownComponent(ConfigurableComponent component) {
        final ConfigurableComponentInitializer initializer;
        if (component instanceof Processor) {
            initializer = new ProcessorInitializer();
        } else if (component instanceof ReportingTask) {
            initializer = new ReportingTaskingInitializer();
        } else if (component instanceof ControllerService) {
            initializer = new ControllerServiceInitializer();
        } else {
            throw new NullPointerException("Unknown type: " + component.getClass());
        }

        initializer.teardown(component);

    }

    private void generateDocumentation(ConfigurableComponent component, ClassLoader classLoader, File docsDirectory) throws FileNotFoundException, IOException {
        final HtmlDocumentationWriter writer;
        if (component instanceof Processor) {
            writer = new HtmlProcessorDocumentationWriter();
        } else if (component instanceof ReportingTask) {
            writer = new HtmlDocumentationWriter();
        } else if (component instanceof ControllerService) {
            writer = new HtmlDocumentationWriter();
        } else {
            throw new NullPointerException("Unknown type: " + component.getClass());
        }

        final String className = component.getClass().getName();

        boolean hasAdditionalDetails = classLoader.getResource("docs" + File.separator + className + File.separator + "additionalDetails.html") != null;
        final File componentDocumentationDir = new File(docsDirectory, className);
        if (!componentDocumentationDir.exists()) {
            componentDocumentationDir.mkdirs();
        }

        final File generatedFile = new File(componentDocumentationDir, "index.html");

        try (FileOutputStream fos = new FileOutputStream(generatedFile); BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            writer.write(component, bos, hasAdditionalDetails);
        }
    }

    private void initializeComponent(ConfigurableComponent component) throws InitializationException {
        final ConfigurableComponentInitializer initializer;
        if (component instanceof Processor) {
            initializer = new ProcessorInitializer();
        } else if (component instanceof ReportingTask) {
            initializer = new ReportingTaskingInitializer();
        } else if (component instanceof ControllerService) {
            initializer = new ControllerServiceInitializer();
        } else {
            throw new NullPointerException("Unknown type: " + component.getClass());
        }

        initializer.initialize(component);
    }
}
