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

import { TestBed, ComponentFixture } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { NiFiCommon, Storage } from '@nifi/shared';
import { ProvenancePreview } from './provenance-preview.component';
import { ProvenanceEvent } from '../../../state/shared';

describe('ProvenancePreview', () => {
    function createMockEvent(overrides: Partial<ProvenanceEvent> = {}): ProvenanceEvent {
        return {
            id: '1',
            eventId: 100,
            eventTime: '01/01/2025 12:00:00 UTC',
            eventType: 'RECEIVE',
            componentId: 'proc-1',
            componentType: 'Processor',
            componentName: 'MyProcessor',
            clusterNodeId: 'node-1',
            clusterNodeAddress: 'node-1.local',
            inputContentAvailable: true,
            outputContentAvailable: true,
            replayAvailable: true,
            sourceConnectionIdentifier: 'conn-1',
            ...overrides
        } as ProvenanceEvent;
    }

    interface SetupOptions {
        events?: ProvenanceEvent[];
        status?: 'pending' | 'loading' | 'success' | 'error';
        error?: string | null;
        connectedToCluster?: boolean;
        contentViewerAvailable?: boolean;
        storageKey?: string;
        collapsed?: boolean;
    }

    async function setup(options: SetupOptions = {}) {
        const mockNiFiCommon = {
            parseDateTime: vi.fn().mockReturnValue(new Date('2025-01-01T12:00:00Z')),
            compareNumber: vi.fn().mockReturnValue(0),
            substringBeforeFirst: vi.fn().mockImplementation((str: string, sep: string) => {
                const i = str.indexOf(sep);
                return i >= 0 ? str.substring(0, i) : str;
            })
        };

        const mockStorage = {
            getItem: vi.fn().mockReturnValue(null),
            setItem: vi.fn(),
            removeItem: vi.fn(),
            hasItem: vi.fn().mockReturnValue(false)
        };

        await TestBed.configureTestingModule({
            imports: [ProvenancePreview, NoopAnimationsModule],
            providers: [
                { provide: NiFiCommon, useValue: mockNiFiCommon },
                { provide: Storage, useValue: mockStorage }
            ]
        }).compileComponents();

        const fixture: ComponentFixture<ProvenancePreview> = TestBed.createComponent(ProvenancePreview);
        const component = fixture.componentInstance;

        fixture.componentRef.setInput('events', options.events ?? []);
        fixture.componentRef.setInput('status', options.status ?? 'pending');

        if (options.error !== undefined) {
            fixture.componentRef.setInput('error', options.error);
        }
        if (options.connectedToCluster !== undefined) {
            fixture.componentRef.setInput('connectedToCluster', options.connectedToCluster);
        }
        if (options.contentViewerAvailable !== undefined) {
            fixture.componentRef.setInput('contentViewerAvailable', options.contentViewerAvailable);
        }
        if (options.storageKey !== undefined) {
            fixture.componentRef.setInput('storageKey', options.storageKey);
        }

        if (options.collapsed !== undefined) {
            component.provenanceCollapsed = options.collapsed;
        }

        fixture.detectChanges();

        return { fixture, component, mockNiFiCommon, mockStorage };
    }

    describe('initialization', () => {
        it('should create the component', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should restore collapsed state from storage', async () => {
            const restoreStorage = {
                getItem: vi.fn().mockReturnValue({ 'provenance-control': true }),
                setItem: vi.fn(),
                removeItem: vi.fn(),
                hasItem: vi.fn().mockReturnValue(true)
            };

            await TestBed.configureTestingModule({
                imports: [ProvenancePreview, NoopAnimationsModule],
                providers: [
                    {
                        provide: NiFiCommon,
                        useValue: {
                            parseDateTime: vi.fn().mockReturnValue(new Date()),
                            compareNumber: vi.fn().mockReturnValue(0),
                            substringBeforeFirst: vi.fn().mockReturnValue('')
                        }
                    },
                    { provide: Storage, useValue: restoreStorage }
                ]
            }).compileComponents();

            const fixture = TestBed.createComponent(ProvenancePreview);
            fixture.componentRef.setInput('events', []);
            fixture.componentRef.setInput('status', 'pending');
            fixture.detectChanges();

            expect(fixture.componentInstance.provenanceCollapsed).toBe(false);
        });
    });

    describe('pending state', () => {
        it('should show "Current selection does not generate events" message', async () => {
            const { fixture } = await setup({ status: 'pending' });
            expect(fixture.nativeElement.textContent).toContain('Current selection does not generate events');
        });
    });

    describe('loading state', () => {
        it('should show skeleton loader', async () => {
            const { fixture } = await setup({
                status: 'loading',
                collapsed: false
            });

            const skeleton = fixture.nativeElement.querySelector('ngx-skeleton-loader');
            expect(skeleton).toBeTruthy();
        });
    });

    describe('error state', () => {
        it('should display the error message', async () => {
            const { fixture } = await setup({
                status: 'error',
                error: 'Network error occurred',
                collapsed: false
            });

            expect(fixture.nativeElement.textContent).toContain('Network error occurred');
        });
    });

    describe('success state', () => {
        it('should display "No recent events" when events array is empty', async () => {
            const { fixture } = await setup({
                status: 'success',
                events: [],
                collapsed: false
            });

            expect(fixture.nativeElement.textContent).toContain('No recent events for current selection');
        });

        it('should display events in the table', async () => {
            const events = [createMockEvent({ eventType: 'RECEIVE' })];
            const { fixture } = await setup({
                status: 'success',
                events,
                collapsed: false
            });

            expect(fixture.nativeElement.textContent).toContain('RECEIVE');
        });
    });

    describe('outputs', () => {
        it('should emit collapsedChange when panel is toggled', async () => {
            const { component } = await setup();
            const spy = vi.fn();
            component.collapsedChange.subscribe(spy);

            component.toggleCollapsed(false);

            expect(spy).toHaveBeenCalledWith(false);
            expect(component.provenanceCollapsed).toBe(false);
        });

        it('should persist collapsed state to storage', async () => {
            const { component, mockStorage } = await setup();

            component.toggleCollapsed(false);

            expect(mockStorage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({ 'provenance-control': true })
            );
        });

        it('should emit refresh on refresh button click and stop event propagation', async () => {
            const { component } = await setup();
            const spy = vi.fn();
            component.refresh.subscribe(spy);

            const event = new MouseEvent('click');
            vi.spyOn(event, 'stopPropagation');
            component.refreshClicked(event);

            expect(spy).toHaveBeenCalled();
            expect(event.stopPropagation).toHaveBeenCalled();
        });

        it('should emit viewDetails when view details is clicked', async () => {
            const { component } = await setup();
            const spy = vi.fn();
            component.viewDetails.subscribe(spy);
            const event = createMockEvent();

            component.viewDetailsClicked(event);

            expect(spy).toHaveBeenCalledWith(event);
        });

        it('should emit downloadContent with direction', async () => {
            const { component } = await setup();
            const spy = vi.fn();
            component.downloadContent.subscribe(spy);
            const event = createMockEvent();

            component.downloadContentClicked(event, 'input');

            expect(spy).toHaveBeenCalledWith({ event, direction: 'input' });
        });

        it('should emit viewContent with direction', async () => {
            const { component } = await setup({ contentViewerAvailable: true });
            const spy = vi.fn();
            component.viewContent.subscribe(spy);
            const event = createMockEvent();

            component.viewContentClicked(event, 'output');

            expect(spy).toHaveBeenCalledWith({ event, direction: 'output' });
        });

        it('should emit replayEvent when replay is clicked', async () => {
            const { component } = await setup();
            const spy = vi.fn();
            component.replayEvent.subscribe(spy);
            const event = createMockEvent();

            component.replayClicked(event);

            expect(spy).toHaveBeenCalledWith(event);
        });
    });

    describe('content availability checks', () => {
        it('should return true for canDownloadContent when input content is available', async () => {
            const { component } = await setup();
            const event = createMockEvent({ inputContentAvailable: true });

            expect(component.canDownloadContent(event, 'input')).toBe(true);
        });

        it('should return false for canDownloadContent when input content is not available', async () => {
            const { component } = await setup();
            const event = createMockEvent({ inputContentAvailable: false });

            expect(component.canDownloadContent(event, 'input')).toBe(false);
        });

        it('should return true for canViewContent when content viewer is available and content exists', async () => {
            const { component } = await setup({ contentViewerAvailable: true });
            const event = createMockEvent({ outputContentAvailable: true });

            expect(component.canViewContent(event, 'output')).toBe(true);
        });

        it('should return false for canViewContent when content viewer is not available', async () => {
            const { component } = await setup({ contentViewerAvailable: false });
            const event = createMockEvent({ outputContentAvailable: true });

            expect(component.canViewContent(event, 'output')).toBe(false);
        });
    });

    describe('row selection', () => {
        it('should select a row', async () => {
            const { component } = await setup();
            const event = createMockEvent();

            component.select(event);

            expect(component.selectedId).toBe(event.id);
        });

        it('should return true for isSelected when the row is selected', async () => {
            const { component } = await setup();
            const event = createMockEvent();

            component.select(event);

            expect(component.isSelected(event)).toBe(true);
        });

        it('should return false for isSelected when no row is selected', async () => {
            const { component } = await setup();
            const event = createMockEvent();

            expect(component.isSelected(event)).toBe(false);
        });
    });

    describe('formatHostname', () => {
        it('should strip the domain from a fully qualified hostname', async () => {
            const { component } = await setup();
            expect(component.formatHostname('node-1.example.com')).toBe('node-1');
        });

        it('should return the address unchanged when there is no separator', async () => {
            const { component } = await setup();
            expect(component.formatHostname('node-1')).toBe('node-1');
        });

        it('should return empty string for undefined address', async () => {
            const { component } = await setup();
            expect(component.formatHostname(undefined)).toBe('');
        });
    });

    describe('custom storageKey', () => {
        it('should use the supplied storageKey for persistence', async () => {
            const { component, mockStorage } = await setup({
                storageKey: 'connector-provenance-control'
            });

            component.toggleCollapsed(false);

            expect(mockStorage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({ 'connector-provenance-control': true })
            );
        });
    });
});
