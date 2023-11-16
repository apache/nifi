import { TestBed } from '@angular/core/testing';

import { EditableBehavior } from './editable-behavior.service';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/flow/flow.reducer';
import { selectFlowState } from '../../state/flow/flow.selectors';
import { CanvasState } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import * as fromFlow from '../../state/flow/flow.reducer';
import { transformFeatureKey } from '../../state/transform';
import * as fromTransform from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';

describe('EditableBehaviorService', () => {
    let service: EditableBehavior;

    const initialState: CanvasState = {
        [flowFeatureKey]: fromFlow.initialState,
        [transformFeatureKey]: fromTransform.initialState
    };

    beforeEach(() => {
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
        service = TestBed.inject(EditableBehavior);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });
});
