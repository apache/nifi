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

import { Injectable } from '@angular/core';
import * as d3 from 'd3';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { CanvasState, ComponentType, Dimension } from '../../state';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import { select, Store } from '@ngrx/store';
import { selectFunnels, selectSelected, selectTransitionRequired } from '../../state/flow/flow.selectors';

@Injectable({providedIn: 'root'})
export class FunnelManager {

    private dimensions: Dimension = {
        width: 48,
        height: 48
    };

    private funnels: string[] = [];
    private funnelContainer: any;
    private transitionRequired: boolean = false;

    constructor(
        private store: Store<CanvasState>,
        private positionBehavior: PositionBehavior,
        private selectableBehavior: SelectableBehavior,
        private editableBehavior: EditableBehavior
    ) {
    }

    private select() {
        return this.funnelContainer.selectAll('g.funnel').data(this.funnels, function (d: any) {
            return d.id;
        });
    }

    private renderFunnels(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        var funnel = entered.append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'funnel component');

        this.positionBehavior.position(funnel, this.transitionRequired);

        // funnel border
        funnel.append('rect')
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
        funnel.append('rect')
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
        funnel.append('text')
            .attr('class', 'funnel-icon')
            .attr('x', 9)
            .attr('y', 34)
            .text('\ue803');

        // always support selection
        this.selectableBehavior.activate(funnel);

        // funnel
        // .call(nfContextMenu.activate);

        return funnel;
    }

    private updateFunnels(updated: any): void {
        if (updated.empty()) {
            return;
        }

        // funnel border authorization
        updated.select('rect.border')
            .classed('unauthorized', function (d: any) {
                return d.permissions.canRead === false;
            });

        // funnel body authorization
        updated.select('rect.body')
            .classed('unauthorized', function (d: any) {
                return d.permissions.canRead === false;
            });

        this.editableBehavior.editable(updated);
    }

    private removeFunnels(removed: any) {
        removed.remove();
    }

    public init(): void {
        this.funnelContainer = d3.select('#canvas').append('g')
            .attr('pointer-events', 'all')
            .attr('class', 'funnels');

        this.store.pipe(select(selectFunnels))
            .subscribe(funnels => {
                this.set(funnels);
            });

        this.store.pipe(select(selectSelected))
            .subscribe(selected => {
                this.funnelContainer.selectAll('g.funnel').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store.pipe(select(selectTransitionRequired))
            .subscribe(transitionRequired => {
                this.transitionRequired = transitionRequired;
            });
    }

    private set(funnels: any): void {
        // update the funnels
        this.funnels = funnels.map((funnel: any) => {
            return {
                ...funnel,
                type: ComponentType.Funnel,
                dimensions: this.dimensions
            }
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
        selection.exit().call(this.removeFunnels);
    }

    public selectAll(): any {
        return this.funnelContainer.selectAll('g.funnel');
    }

    public render(): void {
        this.updateFunnels(d3.selectAll('g.funnel'))
    }

    public pan(): void {
        this.updateFunnels(d3.selectAll('g.funnel.entering, g.funnel.leaving'))
    }
}
