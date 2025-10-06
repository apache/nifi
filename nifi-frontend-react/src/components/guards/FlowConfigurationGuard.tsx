import { ReactNode, useEffect, useMemo, useState } from 'react';
import { useAppDispatch, useAppSelector } from '../../hooks/redux';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { getFlowConfiguration$ } from '../../service/flowConfigurationService';
import {
  setFlowConfigurationStatus,
  setFlowConfiguration,
  setFlowConfigurationError
} from '../../state/flowConfigurationSlice';
import type { FlowConfiguration } from '../../state/flowConfigurationSlice';
import { ErrorResponse, getErrorString } from '../../service/errorHelper';

export interface FlowConfigurationGuardProps {
  check: (flowConfiguration?: FlowConfiguration) => boolean;
  loadingFallback?: ReactNode;
  errorFallback?: (message: string) => ReactNode;
  children: ReactNode;
}

const defaultError = 'Flow configuration check failed.';

function toErrorResponse(error: unknown): ErrorResponse {
  if (typeof error === 'object' && error !== null && 'status' in error) {
    const ajaxError = error as AjaxError;
    return {
      status: ajaxError.status ?? 0,
      message: ajaxError.message,
      error: ajaxError.response
    };
  }

  return {
    status: 0,
    message: error instanceof Error ? error.message : undefined
  };
}

const FlowConfigurationGuard = ({
  check,
  loadingFallback = <div>Loading flow configuration…</div>,
  errorFallback,
  children
}: FlowConfigurationGuardProps) => {
  const dispatch = useAppDispatch();
  const flowConfigState = useAppSelector((state) => state.flowConfiguration);
  const [localError, setLocalError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;

    const loadFlowConfiguration = async () => {
      const shouldLoad =
        flowConfigState.status === 'idle' ||
        (flowConfigState.status === 'error' && !flowConfigState.flowConfiguration);

      if (!shouldLoad) {
        return;
      }

      dispatch(setFlowConfigurationStatus('loading'));
      try {
        const response = await firstValueFrom(getFlowConfiguration$());
        if (!isMounted) return;
        dispatch(setFlowConfiguration(response.flowConfiguration));
        dispatch(setFlowConfigurationStatus('success'));
      } catch (error) {
        if (!isMounted) return;
        const errorResponse = toErrorResponse(error);
        const message =
          getErrorString(errorResponse) ??
          'Unable to load flow configuration. Please contact the system administrator.';
        dispatch(setFlowConfigurationError(message));
        setLocalError(message);
      }
    };

    loadFlowConfiguration();

    return () => {
      isMounted = false;
    };
  }, [dispatch, flowConfigState.status]);

  const isLoading = useMemo(() => {
    return flowConfigState.status === 'idle' || flowConfigState.status === 'loading';
  }, [flowConfigState.status]);

  const flowConfiguration = flowConfigState.flowConfiguration;
  const errorMessage = localError ?? flowConfigState.error ?? null;

  useEffect(() => {
    if (flowConfigState.status === 'success') {
      setLocalError(null);
    }
  }, [flowConfigState.status]);

  if (isLoading) {
    return <>{loadingFallback}</>;
  }

  if (errorMessage) {
    return errorFallback ? errorFallback(errorMessage) : <div>{errorMessage}</div>;
  }

  if (!check(flowConfiguration)) {
    const message = defaultError;
    return errorFallback ? errorFallback(message) : <div>{message}</div>;
  }

  return <>{children}</>;
};

export default FlowConfigurationGuard;
