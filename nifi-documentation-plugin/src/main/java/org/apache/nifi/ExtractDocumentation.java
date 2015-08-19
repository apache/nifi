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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import com.google.common.io.ByteStreams;

@Mojo(name = "extract", defaultPhase = LifecyclePhase.GENERATE_RESOURCES, threadSafe = false, requiresProject = false, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class ExtractDocumentation extends AbstractMojo {
    /**
     * Location of the file.
     */
    @Parameter(defaultValue = "${project.build.directory}", property = "outputDir", required = true)
    private File outputDirectory;

    @Parameter(defaultValue = "${project}", required = true)
    private MavenProject mavenProject;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {

        @SuppressWarnings("unchecked")
        final List<Dependency> runtimeDependencies = mavenProject.getDependencies();

        for (Dependency dependency : runtimeDependencies) {
            getLog().debug("Looking at: " + dependency);
            // TODO dedup
            if (dependency.getType().equals("nar")) {

                final Artifact artifact = (Artifact) mavenProject.getArtifactMap().get(dependency.getGroupId() + ":" + dependency.getArtifactId());
                final File narFile = artifact.getFile();
                try (FileInputStream fos = new FileInputStream(narFile); BufferedInputStream bis = new BufferedInputStream(fos); JarInputStream jis = new JarInputStream(bis)) {

                    JarEntry jarEntry = null;

                    while ((jarEntry = jis.getNextJarEntry()) != null) {
                        if (jarEntry.getName().endsWith(".jar")) {
                            getLog().debug("Found a jar inside of a nar " + jarEntry + " -> " + narFile);
                            processJar(new NoCloseInputStream(jis));
                        }
                    }

                } catch (IOException e) {
                    throw new MojoExecutionException("Unable to process: " + narFile, e);
                }

            }
        }

    }

    private void processJar(InputStream inputStream) throws IOException {
        try (JarInputStream jis = new JarInputStream(inputStream)) {
            JarEntry jarEntry = null;

            while ((jarEntry = jis.getNextJarEntry()) != null) {
                
                if (!jarEntry.isDirectory()) {
                    final String jarName = jarEntry.getName();
                    if (jarName.startsWith("docs")) {
                        getLog().debug("Looking inside a jar at: " + jarEntry);
                        File outputFile = new File(outputDirectory, jarName);
                        File parentFile = outputFile.getParentFile();
                        if (!parentFile.exists()) {
                            parentFile.mkdirs();
                        }

                        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                            ByteStreams.copy(jis, fos);
                        }
                    }
                }
            }
        }
    }

}
