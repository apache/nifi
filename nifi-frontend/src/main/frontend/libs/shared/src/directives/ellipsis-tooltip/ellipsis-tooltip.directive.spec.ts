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

import { Component, Input } from '@angular/core';
import { TestBed, fakeAsync, flushMicrotasks } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatTooltip } from '@angular/material/tooltip';

import { EllipsisTooltipDirective } from './ellipsis-tooltip.directive';

@Component({
    standalone: true,
    imports: [EllipsisTooltipDirective],
    template: `
        <div
            class="truncate"
            ellipsisTooltip
            [tooltipShowDelay]="showDelay"
            [tooltipHideDelay]="hideDelay"
            [tooltipPosition]="position">
            {{ text }}
        </div>
    `
})
class HostComponent {
    @Input() text = 'Some text';
    @Input() showDelay?: number;
    @Input() hideDelay?: number;
    @Input() position?: 'above' | 'below' | 'left' | 'right' | 'before' | 'after';
}

describe('EllipsisTooltipDirective', () => {
    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, HostComponent]
        }).compileComponents();
    });

    it('should set default delays and default position after init', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.detectChanges();

        flushMicrotasks();

        fixture.componentRef.setInput('showDelay', 500);
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const tooltip = debugEl.injector.get(MatTooltip);

        expect(tooltip.showDelay).toBe(500);
        expect(tooltip.hideDelay).toBe(0);
        expect(tooltip.position).toBe('after');
    }));

    it('should disable tooltip when not overflowing', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const host: HTMLElement = debugEl.nativeElement;
        const tooltip = debugEl.injector.get(MatTooltip);

        Object.defineProperty(host, 'offsetWidth', { value: 100, configurable: true });
        Object.defineProperty(host, 'scrollWidth', { value: 90, configurable: true });

        flushMicrotasks();
        fixture.changeDetectorRef.markForCheck();
        fixture.detectChanges();

        expect(tooltip.message).toBe('');
        expect(tooltip.disabled).toBe(true);
    }));

    it('should enable tooltip with trimmed full text when overflowing', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.componentRef.setInput('text', '   Very long label   ');
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const host: HTMLElement = debugEl.nativeElement;
        const tooltip = debugEl.injector.get(MatTooltip);

        Object.defineProperty(host, 'offsetWidth', { value: 50, configurable: true });
        Object.defineProperty(host, 'scrollWidth', { value: 200, configurable: true });

        flushMicrotasks();

        expect(tooltip.message).toBe('Very long label');
        expect(tooltip.disabled).toBe(false);
    }));

    it('should respect provided delays and position inputs', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.componentRef.setInput('showDelay', 200);
        fixture.componentRef.setInput('hideDelay', 300);
        fixture.componentRef.setInput('position', 'above');
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const tooltip = debugEl.injector.get(MatTooltip);

        flushMicrotasks();
        fixture.changeDetectorRef.markForCheck();
        fixture.detectChanges();

        expect(tooltip.showDelay).toBe(200);
        expect(tooltip.hideDelay).toBe(300);
        expect(tooltip.position).toBe('above');
    }));

    it('should enable tooltip when vertically overflowing (line-clamp)', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.componentRef.setInput('text', 'Very long text that will wrap to multiple lines and get clamped');
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const host: HTMLElement = debugEl.nativeElement;
        const tooltip = debugEl.injector.get(MatTooltip);

        Object.defineProperty(host, 'offsetWidth', { value: 200, configurable: true });
        Object.defineProperty(host, 'scrollWidth', { value: 180, configurable: true });
        Object.defineProperty(host, 'offsetHeight', { value: 60, configurable: true });
        Object.defineProperty(host, 'scrollHeight', { value: 120, configurable: true });

        flushMicrotasks();

        expect(tooltip.message).toBe('Very long text that will wrap to multiple lines and get clamped');
        expect(tooltip.disabled).toBe(false);
    }));

    it('should enable tooltip when both horizontally and vertically overflowing', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.componentRef.setInput('text', 'Very long text that overflows both ways');
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const host: HTMLElement = debugEl.nativeElement;
        const tooltip = debugEl.injector.get(MatTooltip);

        Object.defineProperty(host, 'offsetWidth', { value: 50, configurable: true });
        Object.defineProperty(host, 'scrollWidth', { value: 200, configurable: true });
        Object.defineProperty(host, 'offsetHeight', { value: 60, configurable: true });
        Object.defineProperty(host, 'scrollHeight', { value: 120, configurable: true });

        flushMicrotasks();

        expect(tooltip.message).toBe('Very long text that overflows both ways');
        expect(tooltip.disabled).toBe(false);
    }));

    it('should disable tooltip when neither horizontally nor vertically overflowing', fakeAsync(() => {
        const fixture = TestBed.createComponent(HostComponent);
        fixture.detectChanges();

        const debugEl = fixture.debugElement.query(By.directive(EllipsisTooltipDirective));
        const host: HTMLElement = debugEl.nativeElement;
        const tooltip = debugEl.injector.get(MatTooltip);

        Object.defineProperty(host, 'offsetWidth', { value: 200, configurable: true });
        Object.defineProperty(host, 'scrollWidth', { value: 180, configurable: true });
        Object.defineProperty(host, 'offsetHeight', { value: 120, configurable: true });
        Object.defineProperty(host, 'scrollHeight', { value: 100, configurable: true });

        flushMicrotasks();
        fixture.changeDetectorRef.markForCheck();
        fixture.detectChanges();

        expect(tooltip.message).toBe('');
        expect(tooltip.disabled).toBe(true);
    }));
});
