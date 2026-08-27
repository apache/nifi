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

import { hasPropertyValue } from './connector-validation.utils';
import { ConnectorValueReference } from '../types';

describe('hasPropertyValue', () => {
    describe('SECRET type', () => {
        it('should return true for secret reference with value', () => {
            const valueRef: ConnectorValueReference = {
                valueType: 'SECRET_REFERENCE',
                secretName: 'my-secret',
                secretProviderId: 'provider-1',
                secretProviderName: 'AWS Secrets Manager',
                fullyQualifiedSecretName: 'group/my-secret'
            };
            expect(hasPropertyValue(valueRef, 'SECRET')).toBe(true);
        });

        it('should return false for unconfigured secret with providerName only', () => {
            const valueRef: ConnectorValueReference = {
                valueType: 'SECRET_REFERENCE',
                secretProviderName: 'Local Parameter Provider'
            };
            expect(hasPropertyValue(valueRef, 'SECRET')).toBe(false);
        });

        it('should return false for secret reference with all empty strings', () => {
            const valueRef: ConnectorValueReference = {
                valueType: 'SECRET_REFERENCE',
                secretProviderId: '',
                secretProviderName: '',
                fullyQualifiedSecretName: ''
            };
            expect(hasPropertyValue(valueRef, 'SECRET')).toBe(false);
        });

        it('should return true when secretName is set but fullyQualifiedSecretName is not', () => {
            const valueRef: ConnectorValueReference = {
                valueType: 'SECRET_REFERENCE',
                secretProviderName: 'My Provider',
                secretName: 'my-secret'
            };
            expect(hasPropertyValue(valueRef, 'SECRET')).toBe(true);
        });

        it('should return false for cleared secret (STRING_LITERAL with null)', () => {
            const valueRef: ConnectorValueReference = {
                valueType: 'STRING_LITERAL',
                value: null
            };
            expect(hasPropertyValue(valueRef, 'SECRET')).toBe(false);
        });

        it('should return false for undefined value reference', () => {
            expect(hasPropertyValue(undefined, 'SECRET')).toBe(false);
        });
    });
});
