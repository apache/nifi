package org.apache.nifi.security.xml;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* This validator is a quick method to block XML files being used which contain external entity ("!ENTITY") declarations.
 */
public class XXEValidator implements Validator {

    private final Pattern xxePattern = Pattern.compile("<\\s*!ENTITY");

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
        Path xmlFilePath = Paths.get(input);
        String line;
        boolean containsXXE = false;

        if(Files.exists(xmlFilePath)) {
            try (BufferedReader reader = Files.newBufferedReader(xmlFilePath)) {
                while((line = reader.readLine()) != null) {
                    Matcher matcher = xxePattern.matcher(line);

                    if(matcher.find()) {
                        // We found an external entity declaration. Stop reading the file and return an invalid property result.
                        containsXXE = true;
                        break;
                    }
                }

                if(containsXXE) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                            .explanation("XML file " + input + " contained an external entity. To eliminate XXE vulnerabilities, NiFi has external entity processing disabled.")
                            .build();
                } else {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(true).explanation("Does not contain XXE.").build();
                }
            } catch (IOException e) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is not valid because: " + e.getLocalizedMessage())
                        .build();
            }
        } else {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("File not found: " + input + " could not be found.")
                    .build();
        }
    }
}