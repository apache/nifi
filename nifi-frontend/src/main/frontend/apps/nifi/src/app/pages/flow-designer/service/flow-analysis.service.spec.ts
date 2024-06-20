import { TestBed } from '@angular/core/testing';

import { FlowAnalysisService } from './flow-analysis.service';

describe('FlowAnalysisService', () => {
  let service: FlowAnalysisService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(FlowAnalysisService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
