import { TestBed } from '@angular/core/testing';

import { ProcessGroupManager } from './process-group-manager.service';
import { CanvasState } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import * as fromFlow from '../../state/flow/flow.reducer';
import { transformFeatureKey } from '../../state/transform';
import * as fromTransform from '../../state/transform/transform.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../../state/flow/flow.selectors';
import { selectTransform } from '../../state/transform/transform.selectors';

describe('ProcessGroupManager', () => {
    let service: ProcessGroupManager;

    beforeEach(() => {
        const initialState: CanvasState = {
            [flowFeatureKey]: fromFlow.initialState,
            [transformFeatureKey]: fromTransform.initialState
        };

        TestBed.configureTestingModule({
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectFlowState,
                            value: initialState[flowFeatureKey]
                        },
                        {
                            selector: selectTransform,
                            value: initialState[transformFeatureKey]
                        }
                    ]
                })
            ]
        });
        service = TestBed.inject(ProcessGroupManager);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });
});
