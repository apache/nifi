package org.apache.nifi.nar;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;

public class NarUnpackerTest {

	@BeforeClass
	public static void copyResources() throws IOException {

		final Path sourcePath = Paths.get("./src/test/resources");
		final Path targetPath = Paths.get("./target");
		
		Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

				Path relativeSource = sourcePath.relativize(dir);
				Path target = targetPath.resolve(relativeSource);

				Files.createDirectories(target);

				return FileVisitResult.CONTINUE;

			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

				Path relativeSource = sourcePath.relativize(file);
				Path target = targetPath.resolve(relativeSource);

				Files.copy(file, target, REPLACE_EXISTING);

				return FileVisitResult.CONTINUE;
			}
		});
	}

	@Test
	public void testUnpackNars() {

		NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties");

		assertEquals("./target/NarUnpacker/lib/", properties.getProperty("nifi.nar.library.directory"));
		assertEquals("./target/NarUnpacker/lib2/", properties.getProperty("nifi.nar.library.directory.alt"));

		final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);

		assertEquals(2,extensionMapping.getAllExtensionNames().size());

		assertTrue(extensionMapping.getAllExtensionNames().contains("org.apache.nifi.processors.dummy.one"));
		assertTrue(extensionMapping.getAllExtensionNames().contains("org.apache.nifi.processors.dummy.two"));

		final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
		File[] extensionFiles = extensionsWorkingDir.listFiles();
		
		assertEquals(2,extensionFiles.length);
		assertEquals("dummy-one.nar-unpacked", extensionFiles[0].getName());
		assertEquals("dummy-two.nar-unpacked", extensionFiles[1].getName());
	}

	@Test
	public void testUnpackNarsFromEmptyDir() throws IOException {

		NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties");
		
		final File emptyDir = new File ("./target/empty/dir");
		emptyDir.delete();
		emptyDir.deleteOnExit();
		assertTrue(emptyDir.mkdirs());
		
		properties.setProperty("nifi.nar.library.directory.alt", emptyDir.toString());

		final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);

		assertEquals(1,extensionMapping.getAllExtensionNames().size());
		assertTrue(extensionMapping.getAllExtensionNames().contains("org.apache.nifi.processors.dummy.one"));
		
        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
		File[] extensionFiles = extensionsWorkingDir.listFiles();
		
		assertEquals(1,extensionFiles.length);
		assertEquals("dummy-one.nar-unpacked", extensionFiles[0].getName());
	}

	@Test
	public void testUnpackNarsFromNonExistantDir() {
		
		final File nonExistantDir = new File ("./target/this/dir/should/not/exist/");
		nonExistantDir.delete();
		nonExistantDir.deleteOnExit();

		NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties");
		properties.setProperty("nifi.nar.library.directory.alt", nonExistantDir.toString());

		final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);
		
		assertTrue(extensionMapping.getAllExtensionNames().contains("org.apache.nifi.processors.dummy.one"));
		
		assertEquals(1,extensionMapping.getAllExtensionNames().size());
		
        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
		File[] extensionFiles = extensionsWorkingDir.listFiles();
		
		assertEquals(1,extensionFiles.length);
		assertEquals("dummy-one.nar-unpacked", extensionFiles[0].getName());
	}

	@Test
	public void testUnpackNarsFromNonDir() throws IOException {
		
		final File nonDir = new File ("./target/file.txt");
		nonDir.createNewFile();
		nonDir.deleteOnExit();

		NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties");
		properties.setProperty("nifi.nar.library.directory.alt", nonDir.toString());

		final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);
		
		assertNull(extensionMapping);
	}


	private NiFiProperties loadSpecifiedProperties(String propertiesFile) {
		String file = NarUnpackerTest.class.getResource(propertiesFile).getFile();

		System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, file);

		NiFiProperties properties = NiFiProperties.getInstance();

		// clear out existing properties
		for (String prop : properties.stringPropertyNames()) {
			properties.remove(prop);
		}

		InputStream inStream = null;
		try {
			inStream = new BufferedInputStream(new FileInputStream(file));
			properties.load(inStream);
		} catch (final Exception ex) {
			throw new RuntimeException("Cannot load properties file due to " + ex.getLocalizedMessage(), ex);
		} finally {
			if (null != inStream) {
				try {
					inStream.close();
				} catch (final Exception ex) {
					/**
					 * do nothing *
					 */
				}
			}
		}

		return properties;
	}
}