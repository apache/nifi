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
import * as d3 from 'd3'
import { PositionBehavior } from './position-behavior.service';

@Injectable({ providedIn: 'root'})
export class FunnelManager {

  private funnels = new Map<String, any>();
  private funnelContainer;

  constructor(
      private positionBehavior: PositionBehavior
  ) {
    this.funnelContainer = d3.select('#canvas').append('g')
        .attr('pointer-events', 'all')
        .attr('class', 'funnels');
  }

  private select() {
    return this.funnelContainer.selectAll('g.funnel').data(this.funnels.keys(), function (d: any) {
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
        .attr('class', 'funnel component')
        .classed('selected', false)
        .call(this.positionBehavior.position);

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
    // funnel
        // .call(nfSelectable.activate);
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

    updated.each(function () {
      // var funnel = d3.select(this);

      // update the component behavior as appropriate
      // nfCanvasUtils.editable(funnel, nfConnectable, nfDraggable);
    });
  }

  private removeFunnels(removed: any) {
    removed.remove();
  }

  public set(): void {


      // select
      var selection = this.select();

      // enter
      var entered = this.renderFunnels(selection.enter(), /*selectAll*/);

      // update
      var updated = selection.merge(entered);
      updated.call(this.updateFunnels).call(this.positionBehavior.position, /*transition*/ false);

      // exit
      selection.exit().call(this.removeFunnels);
  }
}
