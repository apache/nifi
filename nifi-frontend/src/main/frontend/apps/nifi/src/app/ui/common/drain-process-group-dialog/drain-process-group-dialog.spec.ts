/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { DrainProcessGroupDialog } from './drain-process-group-dialog';
import { DrainDialogState } from './drain-process-group-dialog.types';
import { TextTip } from '@nifi/shared';
import {
    createDrainBlockingComponent,
    createProcessGroupDrainRequest
} from '../../../testing/process-group-drain-fixture';
import { ValidationErrorsTip } from '../tooltips/validation-errors-tip/validation-errors-tip.component';

function createDrainDialogState(overrides: Partial<DrainDialogState> = {}): DrainDialogState {
    return {
        status: 'draining',
        drainRequest: createProcessGroupDrainRequest(),
        ...overrides
    };
}

describe('DrainProcessGroupDialog', () => {
    async function setup(drainState: DrainDialogState) {
        await TestBed.configureTestingModule({
            imports: [DrainProcessGroupDialog]
        }).compileComponents();

        const fixture: ComponentFixture<DrainProcessGroupDialog> = TestBed.createComponent(DrainProcessGroupDialog);
        const component = fixture.componentInstance;
        component.drainState$ = of(drainState);
        fixture.detectChanges();
        return { fixture, component };
    }

    it('uses an indeterminate progress bar until the drain is prepared', async () => {
        const { fixture, component } = await setup(
            createDrainDialogState({
                drainRequest: createProcessGroupDrainRequest({ prepared: false })
            })
        );

        expect(component.isProgressIndeterminate(createProcessGroupDrainRequest({ prepared: false }))).toBe(true);
        expect(fixture.nativeElement.querySelector('mat-progress-bar')).toBeTruthy();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Waiting for the source components to stop'
        );
    });

    it('clamps percent completed between 0 and 100', async () => {
        const { component } = await setup(createDrainDialogState());

        expect(
            component.getPercentCompleted(createProcessGroupDrainRequest({ originalCount: 0, currentCount: 0 }))
        ).toBe(100);
        expect(
            component.getPercentCompleted(createProcessGroupDrainRequest({ originalCount: 10, currentCount: 4 }))
        ).toBe(60);
        expect(
            component.getPercentCompleted(createProcessGroupDrainRequest({ originalCount: 10, currentCount: 20 }))
        ).toBe(0);
    });

    it('shows the finishing message when no FlowFiles remain and the drain is still running', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                drainRequest: createProcessGroupDrainRequest({ currentCount: 0, currentSize: 0 })
            })
        );

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'No FlowFiles remain'
        );
    });

    it('shows Cancel Drain while draining', async () => {
        const { fixture } = await setup(createDrainDialogState({ status: 'draining' }));
        expect(fixture.nativeElement.querySelector('[data-qa="drain-cancel"]')).toBeTruthy();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-close"]')).toBeNull();
    });

    it('shows Close after the drain ends', async () => {
        const { fixture } = await setup(createDrainDialogState({ status: 'ended' }));
        expect(fixture.nativeElement.querySelector('[data-qa="drain-cancel"]')).toBeNull();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-close"]')).toBeTruthy();
    });

    it('does not offer Go To links while draining', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                status: 'draining',
                drainRequest: createProcessGroupDrainRequest({
                    blockingComponents: [createDrainBlockingComponent()]
                })
            })
        );
        expect(fixture.nativeElement.querySelector('[data-qa="drain-go-to"]')).toBeNull();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-blocking-component-label"]').textContent).toContain(
            'GenerateFlowFile'
        );
        expect(
            fixture.nativeElement.querySelector('[data-qa="drain-blocking-component-detail"]').textContent
        ).toContain('is invalid and has 3 FlowFiles queued');
    });

    it('shows a caution banner while draining with blocking components', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                status: 'draining',
                drainRequest: createProcessGroupDrainRequest({
                    blockingComponents: [createDrainBlockingComponent()]
                })
            })
        );

        const banner = fixture.nativeElement.querySelector('[data-qa="drain-blocking-banner"]');
        expect(banner).toBeTruthy();
        expect(banner.textContent).toContain('Drain is blocked');
        expect(banner.textContent).not.toContain('Cancel the drain');
        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Blocked by 1 component'
        );
    });

    it('counts multiple blocking components in the status message', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                status: 'draining',
                drainRequest: createProcessGroupDrainRequest({
                    blockingComponents: [
                        createDrainBlockingComponent({ id: 'a' }),
                        createDrainBlockingComponent({ id: 'b' })
                    ]
                })
            })
        );

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Blocked by 2 components'
        );
    });

    it('shows a processing message while draining without blocking components', async () => {
        const { fixture } = await setup(createDrainDialogState({ status: 'draining' }));

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Processing queued FlowFiles'
        );
    });

    it('offers Go To links once the drain has ended and keeps the blocking banner', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                status: 'ended',
                drainRequest: createProcessGroupDrainRequest({
                    blockingComponents: [createDrainBlockingComponent()]
                })
            })
        );
        expect(fixture.nativeElement.querySelector('[data-qa="drain-go-to"]')).toBeTruthy();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-blocking-banner"]').textContent).toContain(
            'Drain was blocked'
        );
        expect(
            fixture.nativeElement.querySelector('[data-qa="drain-blocking-component-detail"]').textContent
        ).toContain('is invalid and has 3 FlowFiles queued');
    });

    it('uses payload metadata for the row and validation errors for the tooltip', async () => {
        const { component } = await setup(createDrainDialogState());
        const invalid = createDrainBlockingComponent();
        const disabledWithErrors = createDrainBlockingComponent({
            reason: 'DISABLED',
            validationErrors: ["'Upstream Connections' is invalid because Processor requires an upstream connection"],
            queuedCount: 5
        });
        const disabledWithoutErrors = createDrainBlockingComponent({
            reason: 'DISABLED',
            validationErrors: undefined,
            queuedCount: 5
        });
        const unauthorized = createDrainBlockingComponent({ name: undefined, validationErrors: undefined });
        const singleQueued = createDrainBlockingComponent({
            validationErrors: undefined,
            queuedCount: 1
        });

        expect(component.getBlockingTooltipComponent(invalid)).toBe(ValidationErrorsTip);
        expect(component.getBlockingTooltipData(invalid)).toEqual({
            isValidating: false,
            validationErrors: ["'Run Schedule' is invalid"]
        });
        expect(component.getBlockingTooltipComponent(disabledWithErrors)).toBe(ValidationErrorsTip);
        expect(component.getBlockingTooltipData(disabledWithErrors)).toEqual({
            isValidating: false,
            validationErrors: ["'Upstream Connections' is invalid because Processor requires an upstream connection"]
        });
        expect(component.getBlockingTooltipComponent(disabledWithoutErrors)).toBeNull();
        expect(component.getBlockingTooltipComponent(unauthorized)).toBe(TextTip);
        expect(component.getBlockingTooltipData(unauthorized)).toBe("Not authorized to view this component's details.");
        expect(component.getBlockingComponentDetail(invalid)).toBe(' is invalid and has 3 FlowFiles queued');
        expect(component.getBlockingComponentDetail(disabledWithErrors)).toBe(
            ' is disabled and has 5 FlowFiles queued'
        );
        expect(component.getBlockingComponentDetail(unauthorized)).toBe(' is invalid and has 3 FlowFiles queued');
        expect(component.getBlockingComponentDetail(singleQueued)).toBe(' is invalid and has 1 FlowFile queued');
    });

    it('splits a multi-line failure reason for the error banner', async () => {
        const { fixture, component } = await setup(
            createDrainDialogState({
                status: 'ended',
                drainRequest: createProcessGroupDrainRequest({
                    state: 'FAILED',
                    failureReason: 'Node 1 failed\nNode 2 failed'
                })
            })
        );

        expect(component.getFailureMessages('Node 1 failed\nNode 2 failed')).toEqual([
            'Node 1 failed',
            'Node 2 failed'
        ]);
        expect(fixture.nativeElement.querySelector('error-banner')).toBeTruthy();
        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'The drain failed'
        );
    });

    it('falls back to the component id when the name is missing', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                drainRequest: createProcessGroupDrainRequest({
                    blockingComponents: [createDrainBlockingComponent({ id: 'hidden-1', name: undefined })]
                })
            })
        );

        expect(fixture.nativeElement.querySelector('[data-qa="drain-blocking-component-label"]').textContent).toContain(
            'hidden-1'
        );
    });

    it('says the drain is ending and disables Cancel while the cancel request is in flight', async () => {
        const { fixture } = await setup(createDrainDialogState({ status: 'ending' }));

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Ending drain...'
        );
        expect((fixture.nativeElement.querySelector('[data-qa="drain-cancel"]') as HTMLButtonElement).disabled).toBe(
            true
        );
    });

    it('reports that the drain is no longer available when the drain request is gone', async () => {
        const { fixture } = await setup(createDrainDialogState({ status: 'ended', drainRequest: null }));

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'The drain is no longer available.'
        );
    });

    it('reports completion when the ended drain is prepared and empty', async () => {
        const { fixture } = await setup(
            createDrainDialogState({
                status: 'ended',
                drainRequest: createProcessGroupDrainRequest({ state: 'ENDED', prepared: true, currentCount: 0 })
            })
        );

        expect(fixture.nativeElement.querySelector('[data-qa="drain-status-message"]').textContent).toContain(
            'Drain complete. Source components remain stopped.'
        );
    });

    it('emits cancelDrain when Cancel Drain is clicked while draining', async () => {
        const { fixture, component } = await setup(createDrainDialogState({ status: 'draining' }));
        const cancelSpy = vi.fn();
        component.cancelDrain.subscribe(cancelSpy);

        (fixture.nativeElement.querySelector('[data-qa="drain-cancel"]') as HTMLButtonElement).click();

        expect(cancelSpy).toHaveBeenCalledTimes(1);
    });

    it('emits goToComponent with the blocking component when its link is clicked after the drain ends', async () => {
        const blockingComponent = createDrainBlockingComponent({ id: 'processor-9', name: 'Blocked processor' });
        const { fixture, component } = await setup(
            createDrainDialogState({
                status: 'ended',
                drainRequest: createProcessGroupDrainRequest({ blockingComponents: [blockingComponent] })
            })
        );
        const goToSpy = vi.fn();
        component.goToComponent.subscribe(goToSpy);

        (fixture.nativeElement.querySelector('[data-qa="drain-go-to"]') as HTMLElement).click();

        expect(goToSpy).toHaveBeenCalledWith(blockingComponent);
    });
});
