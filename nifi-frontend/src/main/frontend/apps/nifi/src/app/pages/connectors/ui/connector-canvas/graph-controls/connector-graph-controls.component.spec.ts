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

import { Component, input, output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { ConnectorGraphControls } from './connector-graph-controls.component';
import { ConnectorInfoControl } from './connector-info-control/connector-info-control.component';
import { ProvenancePreview } from '../../../../../ui/common/provenance-preview/provenance-preview.component';
import { ConnectorEntity } from '@nifi/shared';
import { BirdseyeComponentData, BirdseyeTransform } from '../../../../../ui/common/birdseye/birdseye.types';
import { Dimension } from '../../../../../ui/common/canvas/canvas.types';
import { ProvenanceEvent } from '../../../../../state/shared';

@Component({
    selector: 'connector-info-control',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockConnectorInfoControl {
    connectorEntity = input<ConnectorEntity | null>(null);
    entitySaving = input<boolean>(false);
}

@Component({
    selector: 'provenance-preview',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockProvenancePreview {
    storageKey = input<string>('provenance-control');
    events = input<ProvenanceEvent[]>([]);
    status = input<'pending' | 'loading' | 'success' | 'error'>('pending');
    error = input<string | null>(null);
    connectedToCluster = input<boolean>(false);
    contentViewerAvailable = input<boolean>(false);
    refresh = output<void>();
    collapsedChange = output<boolean>();
    viewDetails = output<ProvenanceEvent>();
    downloadContent = output<{ event: ProvenanceEvent; direction: 'input' | 'output' }>();
    viewContent = output<{ event: ProvenanceEvent; direction: 'input' | 'output' }>();
    replayEvent = output<ProvenanceEvent>();
}

interface SetupInputs {
    connectorEntity?: ConnectorEntity | null;
    entitySaving?: boolean;
    birdseyeComponents?: BirdseyeComponentData[];
    birdseyeTransform?: BirdseyeTransform;
    canvasDimensions?: Dimension;
    canAccessProvenance?: boolean;
    provenanceEvents?: ProvenanceEvent[];
    provenanceStatus?: 'pending' | 'loading' | 'success' | 'error';
    provenanceError?: string | null;
    connectedToCluster?: boolean;
    contentViewerAvailable?: boolean;
}

async function setup(inputs: SetupInputs = {}) {
    await TestBed.configureTestingModule({
        imports: [ConnectorGraphControls, NoopAnimationsModule]
    })
        .overrideComponent(ConnectorGraphControls, {
            remove: { imports: [ConnectorInfoControl, ProvenancePreview] },
            add: { imports: [MockConnectorInfoControl, MockProvenancePreview] }
        })
        .compileComponents();

    const fixture: ComponentFixture<ConnectorGraphControls> = TestBed.createComponent(ConnectorGraphControls);
    fixture.componentRef.setInput('connectorEntity', inputs.connectorEntity ?? null);
    fixture.componentRef.setInput('entitySaving', inputs.entitySaving ?? false);
    fixture.componentRef.setInput('birdseyeComponents', inputs.birdseyeComponents ?? []);
    fixture.componentRef.setInput(
        'birdseyeTransform',
        inputs.birdseyeTransform ?? { translate: { x: 0, y: 0 }, scale: 1 }
    );
    fixture.componentRef.setInput('canvasDimensions', inputs.canvasDimensions ?? { width: 0, height: 0 });
    fixture.componentRef.setInput('canAccessProvenance', inputs.canAccessProvenance ?? false);
    fixture.componentRef.setInput('provenanceEvents', inputs.provenanceEvents ?? []);
    fixture.componentRef.setInput('provenanceStatus', inputs.provenanceStatus ?? 'pending');
    fixture.componentRef.setInput('provenanceError', inputs.provenanceError ?? null);
    fixture.componentRef.setInput('connectedToCluster', inputs.connectedToCluster ?? false);
    fixture.componentRef.setInput('contentViewerAvailable', inputs.contentViewerAvailable ?? false);
    fixture.detectChanges();

    return { fixture, component: fixture.componentInstance };
}

describe('ConnectorGraphControls', () => {
    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should render the connector-info-control child component', async () => {
        const { fixture } = await setup();
        const infoControl = fixture.nativeElement.querySelector('connector-info-control');
        expect(infoControl).toBeTruthy();
    });

    describe('provenance preview gating', () => {
        it('should NOT render provenance-preview when canAccessProvenance is false', async () => {
            const { fixture } = await setup({ canAccessProvenance: false });
            const preview = fixture.nativeElement.querySelector('provenance-preview');
            expect(preview).toBeNull();
        });

        it('should render provenance-preview when canAccessProvenance is true', async () => {
            const { fixture } = await setup({ canAccessProvenance: true });
            const preview = fixture.nativeElement.querySelector('provenance-preview');
            expect(preview).toBeTruthy();
        });

        it('should propagate provenance inputs to the provenance-preview child', async () => {
            const events = [{ id: 'e1', eventId: 1 } as ProvenanceEvent];
            const { fixture } = await setup({
                canAccessProvenance: true,
                provenanceEvents: events,
                provenanceStatus: 'success',
                provenanceError: 'no error',
                connectedToCluster: true,
                contentViewerAvailable: true
            });
            const previewEl = fixture.debugElement.query((el) => el.name === 'provenance-preview');
            const preview = previewEl.componentInstance as MockProvenancePreview;

            expect(preview.events()).toEqual(events);
            expect(preview.status()).toBe('success');
            expect(preview.error()).toBe('no error');
            expect(preview.connectedToCluster()).toBe(true);
            expect(preview.contentViewerAvailable()).toBe(true);
            expect(preview.storageKey()).toBe('connector-provenance-control');
        });
    });

    describe('provenance preview outputs', () => {
        async function setupWithPreview() {
            const result = await setup({ canAccessProvenance: true });
            const previewEl = result.fixture.debugElement.query((el) => el.name === 'provenance-preview');
            return { ...result, preview: previewEl.componentInstance as MockProvenancePreview };
        }

        it('should re-emit refresh from the provenance-preview', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceRefresh.subscribe(spy);

            preview.refresh.emit();

            expect(spy).toHaveBeenCalled();
        });

        it('should re-emit collapsedChange', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceCollapsedChange.subscribe(spy);

            preview.collapsedChange.emit(false);

            expect(spy).toHaveBeenCalledWith(false);
        });

        it('should re-emit viewDetails', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceViewDetails.subscribe(spy);
            const event = { id: 'evt-1' } as ProvenanceEvent;

            preview.viewDetails.emit(event);

            expect(spy).toHaveBeenCalledWith(event);
        });

        it('should re-emit downloadContent', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceDownloadContent.subscribe(spy);
            const payload = { event: { id: 'evt-1' } as ProvenanceEvent, direction: 'input' as const };

            preview.downloadContent.emit(payload);

            expect(spy).toHaveBeenCalledWith(payload);
        });

        it('should re-emit viewContent', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceViewContent.subscribe(spy);
            const payload = { event: { id: 'evt-1' } as ProvenanceEvent, direction: 'output' as const };

            preview.viewContent.emit(payload);

            expect(spy).toHaveBeenCalledWith(payload);
        });

        it('should re-emit replayEvent', async () => {
            const { component, preview } = await setupWithPreview();
            const spy = vi.fn();
            component.provenanceReplayEvent.subscribe(spy);
            const event = { id: 'evt-1' } as ProvenanceEvent;

            preview.replayEvent.emit(event);

            expect(spy).toHaveBeenCalledWith(event);
        });
    });
});
