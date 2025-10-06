import { ReactNode, useEffect, useMemo, useState } from 'react';
import { useAppDispatch, useAppSelector } from '../../hooks/redux';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { getLoginConfiguration$ } from '../../service/authService';
import {
  setLoginConfigurationStatus,
  setLoginConfiguration,
  setLoginConfigurationError,
  type LoginConfiguration
} from '../../state/loginConfigurationSlice';
import { ErrorResponse, getErrorString } from '../../service/errorHelper';

export interface LoginConfigurationGuardProps {
  check: (loginConfiguration?: LoginConfiguration) => boolean;
  loadingFallback?: ReactNode;
  errorFallback?: (message: string) => ReactNode;
  children: ReactNode;
}

const defaultError = 'Login configuration check failed.';

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

const LoginConfigurationGuard = ({
  check,
  loadingFallback = <div>Loading login configuration…</div>,
  errorFallback,
  children
}: LoginConfigurationGuardProps) => {
  const dispatch = useAppDispatch();
  const loginConfigState = useAppSelector((state) => state.loginConfiguration);
  const [localError, setLocalError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;

    const loadLoginConfiguration = async () => {
      const shouldLoad =
        loginConfigState.status === 'idle' ||
        (loginConfigState.status === 'error' && !loginConfigState.loginConfiguration);

      if (!shouldLoad) {
        return;
      }

      dispatch(setLoginConfigurationStatus('loading'));
      try {
        const response = await firstValueFrom(getLoginConfiguration$());
        if (!isMounted) return;
        dispatch(setLoginConfiguration(response.authenticationConfiguration));
        dispatch(setLoginConfigurationStatus('success'));
      } catch (error) {
        if (!isMounted) return;
        const errorResponse = toErrorResponse(error);
        const message =
          getErrorString(errorResponse) ??
          'Unable to load login configuration. Please contact the system administrator.';
        dispatch(setLoginConfigurationError(message));
        setLocalError(message);
      }
    };

    loadLoginConfiguration();

    return () => {
      isMounted = false;
    };
  }, [dispatch, loginConfigState.loginConfiguration, loginConfigState.status]);

  const isLoading = useMemo(() => {
    return loginConfigState.status === 'idle' || loginConfigState.status === 'loading';
  }, [loginConfigState.status]);

  const loginConfiguration = loginConfigState.loginConfiguration;
  const errorMessage = localError ?? loginConfigState.error ?? null;

  useEffect(() => {
    if (loginConfigState.status === 'success') {
      setLocalError(null);
    }
  }, [loginConfigState.status]);

  if (isLoading) {
    return <>{loadingFallback}</>;
  }

  if (errorMessage) {
    return errorFallback ? errorFallback(errorMessage) : <div>{errorMessage}</div>;
  }

  if (!check(loginConfiguration)) {
    const message = defaultError;
    return errorFallback ? errorFallback(message) : <div>{message}</div>;
  }

  return <>{children}</>;
};

export default LoginConfigurationGuard;
