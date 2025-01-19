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

import { Injectable, OnDestroy } from '@angular/core';
import * as d3 from 'd3';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { CanvasState } from '../../state';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import { Store } from '@ngrx/store';
import {
    selectFlowLoadingStatus,
    selectFunnels,
    selectAnySelectedComponentIds,
    selectTransitionRequired
} from '../../state/flow/flow.selectors';
import { Dimension } from '../../state/shared';
import { ComponentType } from '@nifi/shared';
import { filter, Subject, switchMap, takeUntil } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class FunnelManager implements OnDestroy {
    private destroyed$: Subject<boolean> = new Subject();

    private dimensions: Dimension = {
        width: 48,
        height: 48
    };

    private funnels: [] = [];
    private funnelContainer: any = null;
    private transitionRequired = false;

    constructor(
        private store: Store<CanvasState>,
        private positionBehavior: PositionBehavior,
        private selectableBehavior: SelectableBehavior,
        private editableBehavior: EditableBehavior
    ) {}

    private select() {
        return this.funnelContainer.selectAll('g.funnel').data(this.funnels, function (d: any) {
            return d.id;
        });
    }

    private renderFunnels(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        const funnel = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'funnel component');

        this.positionBehavior.position(funnel, this.transitionRequired);

        // funnel border
        funnel
            .append('rect')
            .attr('rx', 2)
            .attr('ry', 2)
            .attr('class', 'border')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // funnel body
        funnel
            .append('rect')
            .attr('rx', 2)
            .attr('ry', 2)
            .attr('class', 'body')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // funnel icon
        funnel.append('text').attr('class', 'funnel-icon').attr('x', 9).attr('y', 34).text('\ue803');

        this.selectableBehavior.activate(funnel);

        return funnel;
    }

    private updateFunnels(updated: any): void {
        if (updated.empty()) {
            return;
        }

        // funnel border authorization
        updated.select('rect.border').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        // funnel body authorization
        updated.select('rect.body').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        this.editableBehavior.editable(updated);
    }

    private removeFunnels(removed: any) {
        removed.remove();
    }

    public init(): void {
        this.funnelContainer = d3.select('#canvas').append('g').attr('pointer-events', 'all').attr('class', 'funnels');

        this.store
            .select(selectFunnels)
            .pipe(
                filter(() => this.funnelContainer !== null),
                takeUntil(this.destroyed$)
            )
            .subscribe((funnels) => {
                this.set(funnels);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                filter(() => this.funnelContainer !== null),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntil(this.destroyed$)
            )
            .subscribe((selected) => {
                this.funnelContainer.selectAll('g.funnel').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
    }

    public destroy(): void {
        this.funnelContainer = null;
        this.destroyed$.next(true);
    }

    ngOnDestroy(): void {
        this.destroyed$.complete();
    }

    private set(funnels: any): void {
        // update the funnels
        this.funnels = funnels.map((funnel: any) => {
            return {
                ...funnel,
                type: ComponentType.Funnel,
                dimensions: this.dimensions
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderFunnels(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updateFunnels(updated);

        // position
        this.positionBehavior.position(updated, this.transitionRequired);

        // exit
        this.removeFunnels(selection.exit());
    }

    public selectAll(): any {
        return this.funnelContainer.selectAll('g.funnel');
    }

    public render(): void {
        this.updateFunnels(this.selectAll());
    }

    public pan(): void {
        this.updateFunnels(this.funnelContainer.selectAll('g.funnel.entering, g.funnel.leaving'));
    }
}
