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

import { DestroyRef } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { CONNECTOR_MESSAGE_NAMESPACE } from '@nifi/shared';
import { ConnectorMessageHost, ConnectorMessageHostOptions } from './connector-message-host.service';

describe('ConnectorMessageHost', () => {
    const TRUSTED_ORIGIN = 'http://localhost:4200';

    function createMockDestroyRef(): DestroyRef {
        const callbacks: Array<() => void> = [];
        return {
            onDestroy: (cb: () => void) => {
                callbacks.push(cb);
                return () => {
                    // unregister callback (no-op in tests)
                };
            },
            _destroy: () => callbacks.forEach((cb) => cb())
        } as unknown as DestroyRef;
    }

    interface SetupOptions {
        navigate?: ReturnType<typeof vi.fn>;
    }

    async function setup(options: SetupOptions = {}) {
        const navigate = options.navigate ?? vi.fn().mockResolvedValue(true);

        await TestBed.configureTestingModule({
            providers: [ConnectorMessageHost, { provide: Router, useValue: { navigate } }]
        }).compileComponents();

        const service = TestBed.inject(ConnectorMessageHost);
        const router = TestBed.inject(Router) as { navigate: ReturnType<typeof vi.fn> };

        const destroyRef = createMockDestroyRef();

        return { service, router, navigate, destroyRef };
    }

    function createDefaultOptions(destroyRef: DestroyRef): ConnectorMessageHostOptions {
        return {
            destroyRef,
            expectedOrigin: TRUSTED_ORIGIN
        };
    }

    function dispatchMessageEvent(data: unknown, origin: string = TRUSTED_ORIGIN, source: Window | null = null): void {
        const event = new MessageEvent('message', { data, origin, source });
        window.dispatchEvent(event);
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('navigate-to-connector-listing', () => {
        it('should navigate to connector detail when connectorId is provided', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'abc-123' }
            });

            expect(navigate).toHaveBeenCalledWith(['/connectors', 'abc-123']);
        });

        it('should navigate to connectors listing when connectorId is not provided', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: {}
            });

            expect(navigate).toHaveBeenCalledWith(['/connectors']);
        });
    });

    describe('non-navigation message types', () => {
        it('should not navigate for step-saved', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'step-saved',
                payload: { connectorId: 'c1', stepName: 'Step 1', stepIndex: 0 }
            });

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should not navigate for verify-error', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'verify-error',
                payload: { connectorId: 'c1', stepName: 'Step 1', error: 'Timeout' }
            });

            expect(navigate).not.toHaveBeenCalled();
        });
    });

    describe('origin validation', () => {
        it('should reject messages from a different origin', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c1' }
                },
                'https://malicious-site.example.com'
            );

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should reject messages with empty origin when expectedOrigin is set', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c1' }
                },
                ''
            );

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should accept navigate messages from the expected origin', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c1' }
                },
                TRUSTED_ORIGIN
            );

            expect(navigate).toHaveBeenCalledWith(['/connectors', 'c1']);
        });
    });

    describe('source validation', () => {
        it('should reject messages when source does not match iframe contentWindow', async () => {
            const { service, navigate, destroyRef } = await setup();

            const mockIframe = {
                contentWindow: { name: 'expected-window' }
            } as unknown as HTMLIFrameElement;

            service.startListening({
                ...createDefaultOptions(destroyRef),
                iframeElement: () => mockIframe
            });

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should accept messages when iframeElement getter returns undefined (iframe not yet rendered)', async () => {
            const { service, navigate, destroyRef } = await setup();

            service.startListening({
                ...createDefaultOptions(destroyRef),
                iframeElement: () => undefined
            });

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).toHaveBeenCalledWith(['/connectors', 'c1']);
        });

        it('should accept messages when no iframeElement getter is provided', async () => {
            const { service, navigate, destroyRef } = await setup();

            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).toHaveBeenCalledWith(['/connectors', 'c1']);
        });
    });

    describe('connector-ui-ready', () => {
        it('should invoke onConnectorUiReady when the iframe posts a valid connector-ui-ready message', async () => {
            const onConnectorUiReady = vi.fn();
            const { service, destroyRef } = await setup();

            service.startListening({
                ...createDefaultOptions(destroyRef),
                onConnectorUiReady
            });

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'connector-ui-ready',
                payload: { connectorId: 'c1' }
            });

            expect(onConnectorUiReady).toHaveBeenCalledTimes(1);
            expect(onConnectorUiReady.mock.calls[0][0]).toBeInstanceOf(MessageEvent);
        });

        it('should not invoke onConnectorUiReady when origin does not match expectedOrigin', async () => {
            const onConnectorUiReady = vi.fn();
            const { service, destroyRef } = await setup();

            service.startListening({
                ...createDefaultOptions(destroyRef),
                onConnectorUiReady
            });

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'connector-ui-ready',
                    payload: {}
                },
                'https://evil.example.com'
            );

            expect(onConnectorUiReady).not.toHaveBeenCalled();
        });

        it('should not invoke onConnectorUiReady when namespace is not the connector message namespace', async () => {
            const onConnectorUiReady = vi.fn();
            const { service, destroyRef } = await setup();

            service.startListening({
                ...createDefaultOptions(destroyRef),
                onConnectorUiReady
            });

            dispatchMessageEvent({
                namespace: 'other-namespace',
                type: 'connector-ui-ready',
                payload: {}
            });

            expect(onConnectorUiReady).not.toHaveBeenCalled();
        });

        it('should not navigate for connector-ui-ready', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'connector-ui-ready',
                payload: {}
            });

            expect(navigate).not.toHaveBeenCalled();
        });
    });

    describe('message filtering', () => {
        it('should ignore messages with wrong namespace', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: 'wrong-namespace',
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should ignore messages without namespace', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should ignore non-object messages', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent('just a string');

            expect(navigate).not.toHaveBeenCalled();
        });

        it('should ignore null messages', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent(null);

            expect(navigate).not.toHaveBeenCalled();
        });
    });

    describe('extractOrigin', () => {
        it('should extract origin from a valid URL', () => {
            expect(ConnectorMessageHost.extractOrigin('https://connector-ui.example.com/config?id=123')).toBe(
                'https://connector-ui.example.com'
            );
        });

        it('should extract origin including port', () => {
            expect(ConnectorMessageHost.extractOrigin('http://localhost:4200/wizard')).toBe('http://localhost:4200');
        });

        it('should return empty string for invalid URL', () => {
            expect(ConnectorMessageHost.extractOrigin('not-a-url')).toBe('');
        });

        it('should return empty string for empty string', () => {
            expect(ConnectorMessageHost.extractOrigin('')).toBe('');
        });
    });

    describe('lifecycle cleanup', () => {
        it('should stop listening when destroyRef fires', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });
            expect(navigate).toHaveBeenCalledTimes(1);

            (destroyRef as unknown as { _destroy: () => void })._destroy();

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c2' }
            });
            expect(navigate).toHaveBeenCalledTimes(1);
        });

        it('should tear down previous listener when startListening is called again', async () => {
            const { service, navigate, destroyRef } = await setup();

            const firstOrigin = 'http://first-origin.example.com';
            const secondOrigin = 'http://second-origin.example.com';

            service.startListening({
                destroyRef,
                expectedOrigin: firstOrigin
            });

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c1' }
                },
                firstOrigin
            );
            expect(navigate).toHaveBeenCalledTimes(1);

            service.startListening({
                destroyRef,
                expectedOrigin: secondOrigin
            });

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c2' }
                },
                firstOrigin
            );
            expect(navigate).toHaveBeenCalledTimes(1);

            dispatchMessageEvent(
                {
                    namespace: CONNECTOR_MESSAGE_NAMESPACE,
                    type: 'navigate-to-connector-listing',
                    payload: { connectorId: 'c3' }
                },
                secondOrigin
            );
            expect(navigate).toHaveBeenCalledTimes(2);
        });

        it('should not duplicate handlers when startListening is called multiple times', async () => {
            const { service, navigate, destroyRef } = await setup();

            service.startListening(createDefaultOptions(destroyRef));
            service.startListening(createDefaultOptions(destroyRef));
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });

            expect(navigate).toHaveBeenCalledTimes(1);
        });

        it('should stop listening when stopListening is called explicitly', async () => {
            const { service, navigate, destroyRef } = await setup();
            service.startListening(createDefaultOptions(destroyRef));

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c1' }
            });
            expect(navigate).toHaveBeenCalledTimes(1);

            service.stopListening();

            dispatchMessageEvent({
                namespace: CONNECTOR_MESSAGE_NAMESPACE,
                type: 'navigate-to-connector-listing',
                payload: { connectorId: 'c2' }
            });
            expect(navigate).toHaveBeenCalledTimes(1);
        });

        it('should be safe to call stopListening when not listening', async () => {
            const { service } = await setup();
            expect(() => service.stopListening()).not.toThrow();
        });
    });
});
