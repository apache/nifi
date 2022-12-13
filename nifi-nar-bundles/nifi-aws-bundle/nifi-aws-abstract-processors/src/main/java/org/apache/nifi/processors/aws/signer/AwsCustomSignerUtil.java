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
package org.apache.nifi.processors.aws.signer;

import com.amazonaws.auth.Signer;
import com.amazonaws.auth.SignerFactory;
import org.apache.nifi.processor.exception.ProcessException;

public final class AwsCustomSignerUtil {

    private AwsCustomSignerUtil() {
        // util class' constructor
    }

    @SuppressWarnings("unchecked")
    public static String registerCustomSigner(final String className) {
        final Class<? extends Signer> signerClass;

        try {
            final Class<?> clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());

            if (Signer.class.isAssignableFrom(clazz)) {
                signerClass = (Class<? extends Signer>) clazz;
            } else {
                throw new ProcessException(String.format("Cannot create signer from class %s because it does not implement %s", className, Signer.class.getName()));
            }
        } catch (ClassNotFoundException cnfe) {
            throw new ProcessException("Signer class not found: " + className);
        } catch (Exception e) {
            throw new ProcessException("Error while creating signer from class: " + className);
        }

        String signerName = signerClass.getName();

        SignerFactory.registerSigner(signerName, signerClass);

        return signerName;
    }
}
