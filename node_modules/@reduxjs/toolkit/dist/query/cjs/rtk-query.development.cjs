"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/query/index.ts
var query_exports = {};
__export(query_exports, {
  NamedSchemaError: () => NamedSchemaError,
  QueryStatus: () => QueryStatus,
  _NEVER: () => _NEVER,
  buildCreateApi: () => buildCreateApi,
  copyWithStructuralSharing: () => copyWithStructuralSharing,
  coreModule: () => coreModule,
  coreModuleName: () => coreModuleName,
  createApi: () => createApi,
  defaultSerializeQueryArgs: () => defaultSerializeQueryArgs,
  fakeBaseQuery: () => fakeBaseQuery,
  fetchBaseQuery: () => fetchBaseQuery,
  retry: () => retry,
  setupListeners: () => setupListeners,
  skipToken: () => skipToken
});
module.exports = __toCommonJS(query_exports);

// src/query/core/apiState.ts
var QueryStatus = /* @__PURE__ */ ((QueryStatus2) => {
  QueryStatus2["uninitialized"] = "uninitialized";
  QueryStatus2["pending"] = "pending";
  QueryStatus2["fulfilled"] = "fulfilled";
  QueryStatus2["rejected"] = "rejected";
  return QueryStatus2;
})(QueryStatus || {});
function getRequestStatusFlags(status) {
  return {
    status,
    isUninitialized: status === "uninitialized" /* uninitialized */,
    isLoading: status === "pending" /* pending */,
    isSuccess: status === "fulfilled" /* fulfilled */,
    isError: status === "rejected" /* rejected */
  };
}

// src/query/core/rtkImports.ts
var import_toolkit = require("@reduxjs/toolkit");

// src/query/utils/copyWithStructuralSharing.ts
var isPlainObject2 = import_toolkit.isPlainObject;
function copyWithStructuralSharing(oldObj, newObj) {
  if (oldObj === newObj || !(isPlainObject2(oldObj) && isPlainObject2(newObj) || Array.isArray(oldObj) && Array.isArray(newObj))) {
    return newObj;
  }
  const newKeys = Object.keys(newObj);
  const oldKeys = Object.keys(oldObj);
  let isSameObject = newKeys.length === oldKeys.length;
  const mergeObj = Array.isArray(newObj) ? [] : {};
  for (const key of newKeys) {
    mergeObj[key] = copyWithStructuralSharing(oldObj[key], newObj[key]);
    if (isSameObject) isSameObject = oldObj[key] === mergeObj[key];
  }
  return isSameObject ? oldObj : mergeObj;
}

// src/query/utils/countObjectKeys.ts
function countObjectKeys(obj) {
  let count = 0;
  for (const _key in obj) {
    count++;
  }
  return count;
}

// src/query/utils/flatten.ts
var flatten = (arr) => [].concat(...arr);

// src/query/utils/isAbsoluteUrl.ts
function isAbsoluteUrl(url) {
  return new RegExp(`(^|:)//`).test(url);
}

// src/query/utils/isDocumentVisible.ts
function isDocumentVisible() {
  if (typeof document === "undefined") {
    return true;
  }
  return document.visibilityState !== "hidden";
}

// src/query/utils/isNotNullish.ts
function isNotNullish(v) {
  return v != null;
}

// src/query/utils/isOnline.ts
function isOnline() {
  return typeof navigator === "undefined" ? true : navigator.onLine === void 0 ? true : navigator.onLine;
}

// src/query/utils/joinUrls.ts
var withoutTrailingSlash = (url) => url.replace(/\/$/, "");
var withoutLeadingSlash = (url) => url.replace(/^\//, "");
function joinUrls(base, url) {
  if (!base) {
    return url;
  }
  if (!url) {
    return base;
  }
  if (isAbsoluteUrl(url)) {
    return url;
  }
  const delimiter = base.endsWith("/") || !url.startsWith("?") ? "/" : "";
  base = withoutTrailingSlash(base);
  url = withoutLeadingSlash(url);
  return `${base}${delimiter}${url}`;
}

// src/query/utils/getOrInsert.ts
function getOrInsert(map, key, value) {
  if (map.has(key)) return map.get(key);
  return map.set(key, value).get(key);
}
function getOrInsertComputed(map, key, compute) {
  if (map.has(key)) return map.get(key);
  return map.set(key, compute(key)).get(key);
}
var createNewMap = () => /* @__PURE__ */ new Map();

// src/query/fetchBaseQuery.ts
var defaultFetchFn = (...args) => fetch(...args);
var defaultValidateStatus = (response) => response.status >= 200 && response.status <= 299;
var defaultIsJsonContentType = (headers) => (
  /*applicat*/
  /ion\/(vnd\.api\+)?json/.test(headers.get("content-type") || "")
);
function stripUndefined(obj) {
  if (!(0, import_toolkit.isPlainObject)(obj)) {
    return obj;
  }
  const copy = {
    ...obj
  };
  for (const [k, v] of Object.entries(copy)) {
    if (v === void 0) delete copy[k];
  }
  return copy;
}
function fetchBaseQuery({
  baseUrl,
  prepareHeaders = (x) => x,
  fetchFn = defaultFetchFn,
  paramsSerializer,
  isJsonContentType = defaultIsJsonContentType,
  jsonContentType = "application/json",
  jsonReplacer,
  timeout: defaultTimeout,
  responseHandler: globalResponseHandler,
  validateStatus: globalValidateStatus,
  ...baseFetchOptions
} = {}) {
  if (typeof fetch === "undefined" && fetchFn === defaultFetchFn) {
    console.warn("Warning: `fetch` is not available. Please supply a custom `fetchFn` property to use `fetchBaseQuery` on SSR environments.");
  }
  return async (arg, api, extraOptions) => {
    const {
      getState,
      extra,
      endpoint,
      forced,
      type
    } = api;
    let meta;
    let {
      url,
      headers = new Headers(baseFetchOptions.headers),
      params = void 0,
      responseHandler = globalResponseHandler ?? "json",
      validateStatus = globalValidateStatus ?? defaultValidateStatus,
      timeout = defaultTimeout,
      ...rest
    } = typeof arg == "string" ? {
      url: arg
    } : arg;
    let abortController, signal = api.signal;
    if (timeout) {
      abortController = new AbortController();
      api.signal.addEventListener("abort", abortController.abort);
      signal = abortController.signal;
    }
    let config = {
      ...baseFetchOptions,
      signal,
      ...rest
    };
    headers = new Headers(stripUndefined(headers));
    config.headers = await prepareHeaders(headers, {
      getState,
      arg,
      extra,
      endpoint,
      forced,
      type,
      extraOptions
    }) || headers;
    const isJsonifiable = (body) => typeof body === "object" && ((0, import_toolkit.isPlainObject)(body) || Array.isArray(body) || typeof body.toJSON === "function");
    if (!config.headers.has("content-type") && isJsonifiable(config.body)) {
      config.headers.set("content-type", jsonContentType);
    }
    if (isJsonifiable(config.body) && isJsonContentType(config.headers)) {
      config.body = JSON.stringify(config.body, jsonReplacer);
    }
    if (params) {
      const divider = ~url.indexOf("?") ? "&" : "?";
      const query = paramsSerializer ? paramsSerializer(params) : new URLSearchParams(stripUndefined(params));
      url += divider + query;
    }
    url = joinUrls(baseUrl, url);
    const request = new Request(url, config);
    const requestClone = new Request(url, config);
    meta = {
      request: requestClone
    };
    let response, timedOut = false, timeoutId = abortController && setTimeout(() => {
      timedOut = true;
      abortController.abort();
    }, timeout);
    try {
      response = await fetchFn(request);
    } catch (e) {
      return {
        error: {
          status: timedOut ? "TIMEOUT_ERROR" : "FETCH_ERROR",
          error: String(e)
        },
        meta
      };
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
      abortController?.signal.removeEventListener("abort", abortController.abort);
    }
    const responseClone = response.clone();
    meta.response = responseClone;
    let resultData;
    let responseText = "";
    try {
      let handleResponseError;
      await Promise.all([
        handleResponse(response, responseHandler).then((r) => resultData = r, (e) => handleResponseError = e),
        // see https://github.com/node-fetch/node-fetch/issues/665#issuecomment-538995182
        // we *have* to "use up" both streams at the same time or they will stop running in node-fetch scenarios
        responseClone.text().then((r) => responseText = r, () => {
        })
      ]);
      if (handleResponseError) throw handleResponseError;
    } catch (e) {
      return {
        error: {
          status: "PARSING_ERROR",
          originalStatus: response.status,
          data: responseText,
          error: String(e)
        },
        meta
      };
    }
    return validateStatus(response, resultData) ? {
      data: resultData,
      meta
    } : {
      error: {
        status: response.status,
        data: resultData
      },
      meta
    };
  };
  async function handleResponse(response, responseHandler) {
    if (typeof responseHandler === "function") {
      return responseHandler(response);
    }
    if (responseHandler === "content-type") {
      responseHandler = isJsonContentType(response.headers) ? "json" : "text";
    }
    if (responseHandler === "json") {
      const text = await response.text();
      return text.length ? JSON.parse(text) : null;
    }
    return response.text();
  }
}

// src/query/HandledError.ts
var HandledError = class {
  constructor(value, meta = void 0) {
    this.value = value;
    this.meta = meta;
  }
};

// src/query/retry.ts
async function defaultBackoff(attempt = 0, maxRetries = 5) {
  const attempts = Math.min(attempt, maxRetries);
  const timeout = ~~((Math.random() + 0.4) * (300 << attempts));
  await new Promise((resolve) => setTimeout((res) => resolve(res), timeout));
}
function fail(error, meta) {
  throw Object.assign(new HandledError({
    error,
    meta
  }), {
    throwImmediately: true
  });
}
var EMPTY_OPTIONS = {};
var retryWithBackoff = (baseQuery, defaultOptions) => async (args, api, extraOptions) => {
  const possibleMaxRetries = [5, (defaultOptions || EMPTY_OPTIONS).maxRetries, (extraOptions || EMPTY_OPTIONS).maxRetries].filter((x) => x !== void 0);
  const [maxRetries] = possibleMaxRetries.slice(-1);
  const defaultRetryCondition = (_, __, {
    attempt
  }) => attempt <= maxRetries;
  const options = {
    maxRetries,
    backoff: defaultBackoff,
    retryCondition: defaultRetryCondition,
    ...defaultOptions,
    ...extraOptions
  };
  let retry2 = 0;
  while (true) {
    try {
      const result = await baseQuery(args, api, extraOptions);
      if (result.error) {
        throw new HandledError(result);
      }
      return result;
    } catch (e) {
      retry2++;
      if (e.throwImmediately) {
        if (e instanceof HandledError) {
          return e.value;
        }
        throw e;
      }
      if (e instanceof HandledError && !options.retryCondition(e.value.error, args, {
        attempt: retry2,
        baseQueryApi: api,
        extraOptions
      })) {
        return e.value;
      }
      await options.backoff(retry2, options.maxRetries);
    }
  }
};
var retry = /* @__PURE__ */ Object.assign(retryWithBackoff, {
  fail
});

// src/query/core/setupListeners.ts
var onFocus = /* @__PURE__ */ (0, import_toolkit.createAction)("__rtkq/focused");
var onFocusLost = /* @__PURE__ */ (0, import_toolkit.createAction)("__rtkq/unfocused");
var onOnline = /* @__PURE__ */ (0, import_toolkit.createAction)("__rtkq/online");
var onOffline = /* @__PURE__ */ (0, import_toolkit.createAction)("__rtkq/offline");
var initialized = false;
function setupListeners(dispatch, customHandler) {
  function defaultHandler() {
    const handleFocus = () => dispatch(onFocus());
    const handleFocusLost = () => dispatch(onFocusLost());
    const handleOnline = () => dispatch(onOnline());
    const handleOffline = () => dispatch(onOffline());
    const handleVisibilityChange = () => {
      if (window.document.visibilityState === "visible") {
        handleFocus();
      } else {
        handleFocusLost();
      }
    };
    if (!initialized) {
      if (typeof window !== "undefined" && window.addEventListener) {
        window.addEventListener("visibilitychange", handleVisibilityChange, false);
        window.addEventListener("focus", handleFocus, false);
        window.addEventListener("online", handleOnline, false);
        window.addEventListener("offline", handleOffline, false);
        initialized = true;
      }
    }
    const unsubscribe = () => {
      window.removeEventListener("focus", handleFocus);
      window.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
      initialized = false;
    };
    return unsubscribe;
  }
  return customHandler ? customHandler(dispatch, {
    onFocus,
    onFocusLost,
    onOffline,
    onOnline
  }) : defaultHandler();
}

// src/query/endpointDefinitions.ts
function isQueryDefinition(e) {
  return e.type === "query" /* query */;
}
function isMutationDefinition(e) {
  return e.type === "mutation" /* mutation */;
}
function isInfiniteQueryDefinition(e) {
  return e.type === "infinitequery" /* infinitequery */;
}
function isAnyQueryDefinition(e) {
  return isQueryDefinition(e) || isInfiniteQueryDefinition(e);
}
function calculateProvidedBy(description, result, error, queryArg, meta, assertTagTypes) {
  if (isFunction(description)) {
    return description(result, error, queryArg, meta).filter(isNotNullish).map(expandTagDescription).map(assertTagTypes);
  }
  if (Array.isArray(description)) {
    return description.map(expandTagDescription).map(assertTagTypes);
  }
  return [];
}
function isFunction(t) {
  return typeof t === "function";
}
function expandTagDescription(description) {
  return typeof description === "string" ? {
    type: description
  } : description;
}

// src/query/core/buildThunks.ts
var import_immer = require("immer");

// src/query/core/buildInitiate.ts
var import_toolkit2 = require("@reduxjs/toolkit");

// src/tsHelpers.ts
function asSafePromise(promise, fallback) {
  return promise.catch(fallback);
}

// src/query/core/buildInitiate.ts
var forceQueryFnSymbol = Symbol("forceQueryFn");
var isUpsertQuery = (arg) => typeof arg[forceQueryFnSymbol] === "function";
function buildInitiate({
  serializeQueryArgs,
  queryThunk,
  infiniteQueryThunk,
  mutationThunk,
  api,
  context,
  internalState
}) {
  const {
    runningQueries,
    runningMutations
  } = internalState;
  const {
    unsubscribeQueryResult,
    removeMutationResult,
    updateSubscriptionOptions
  } = api.internalActions;
  return {
    buildInitiateQuery,
    buildInitiateInfiniteQuery,
    buildInitiateMutation,
    getRunningQueryThunk,
    getRunningMutationThunk,
    getRunningQueriesThunk,
    getRunningMutationsThunk
  };
  function getRunningQueryThunk(endpointName, queryArgs) {
    return (dispatch) => {
      const endpointDefinition = context.endpointDefinitions[endpointName];
      const queryCacheKey = serializeQueryArgs({
        queryArgs,
        endpointDefinition,
        endpointName
      });
      return runningQueries.get(dispatch)?.[queryCacheKey];
    };
  }
  function getRunningMutationThunk(_endpointName, fixedCacheKeyOrRequestId) {
    return (dispatch) => {
      return runningMutations.get(dispatch)?.[fixedCacheKeyOrRequestId];
    };
  }
  function getRunningQueriesThunk() {
    return (dispatch) => Object.values(runningQueries.get(dispatch) || {}).filter(isNotNullish);
  }
  function getRunningMutationsThunk() {
    return (dispatch) => Object.values(runningMutations.get(dispatch) || {}).filter(isNotNullish);
  }
  function middlewareWarning(dispatch) {
    if (true) {
      if (middlewareWarning.triggered) return;
      const returnedValue = dispatch(api.internalActions.internal_getRTKQSubscriptions());
      middlewareWarning.triggered = true;
      if (typeof returnedValue !== "object" || typeof returnedValue?.type === "string") {
        throw new Error(false ? _formatProdErrorMessage(34) : `Warning: Middleware for RTK-Query API at reducerPath "${api.reducerPath}" has not been added to the store.
You must add the middleware for RTK-Query to function correctly!`);
      }
    }
  }
  function buildInitiateAnyQuery(endpointName, endpointDefinition) {
    const queryAction = (arg, {
      subscribe = true,
      forceRefetch,
      subscriptionOptions,
      [forceQueryFnSymbol]: forceQueryFn,
      ...rest
    } = {}) => (dispatch, getState) => {
      const queryCacheKey = serializeQueryArgs({
        queryArgs: arg,
        endpointDefinition,
        endpointName
      });
      let thunk;
      const commonThunkArgs = {
        ...rest,
        type: "query",
        subscribe,
        forceRefetch,
        subscriptionOptions,
        endpointName,
        originalArgs: arg,
        queryCacheKey,
        [forceQueryFnSymbol]: forceQueryFn
      };
      if (isQueryDefinition(endpointDefinition)) {
        thunk = queryThunk(commonThunkArgs);
      } else {
        const {
          direction,
          initialPageParam
        } = rest;
        thunk = infiniteQueryThunk({
          ...commonThunkArgs,
          // Supply these even if undefined. This helps with a field existence
          // check over in `buildSlice.ts`
          direction,
          initialPageParam
        });
      }
      const selector = api.endpoints[endpointName].select(arg);
      const thunkResult = dispatch(thunk);
      const stateAfter = selector(getState());
      middlewareWarning(dispatch);
      const {
        requestId,
        abort
      } = thunkResult;
      const skippedSynchronously = stateAfter.requestId !== requestId;
      const runningQuery = runningQueries.get(dispatch)?.[queryCacheKey];
      const selectFromState = () => selector(getState());
      const statePromise = Object.assign(forceQueryFn ? (
        // a query has been forced (upsertQueryData)
        // -> we want to resolve it once data has been written with the data that will be written
        thunkResult.then(selectFromState)
      ) : skippedSynchronously && !runningQuery ? (
        // a query has been skipped due to a condition and we do not have any currently running query
        // -> we want to resolve it immediately with the current data
        Promise.resolve(stateAfter)
      ) : (
        // query just started or one is already in flight
        // -> wait for the running query, then resolve with data from after that
        Promise.all([runningQuery, thunkResult]).then(selectFromState)
      ), {
        arg,
        requestId,
        subscriptionOptions,
        queryCacheKey,
        abort,
        async unwrap() {
          const result = await statePromise;
          if (result.isError) {
            throw result.error;
          }
          return result.data;
        },
        refetch: () => dispatch(queryAction(arg, {
          subscribe: false,
          forceRefetch: true
        })),
        unsubscribe() {
          if (subscribe) dispatch(unsubscribeQueryResult({
            queryCacheKey,
            requestId
          }));
        },
        updateSubscriptionOptions(options) {
          statePromise.subscriptionOptions = options;
          dispatch(updateSubscriptionOptions({
            endpointName,
            requestId,
            queryCacheKey,
            options
          }));
        }
      });
      if (!runningQuery && !skippedSynchronously && !forceQueryFn) {
        const running = getOrInsert(runningQueries, dispatch, {});
        running[queryCacheKey] = statePromise;
        statePromise.then(() => {
          delete running[queryCacheKey];
          if (!countObjectKeys(running)) {
            runningQueries.delete(dispatch);
          }
        });
      }
      return statePromise;
    };
    return queryAction;
  }
  function buildInitiateQuery(endpointName, endpointDefinition) {
    const queryAction = buildInitiateAnyQuery(endpointName, endpointDefinition);
    return queryAction;
  }
  function buildInitiateInfiniteQuery(endpointName, endpointDefinition) {
    const infiniteQueryAction = buildInitiateAnyQuery(endpointName, endpointDefinition);
    return infiniteQueryAction;
  }
  function buildInitiateMutation(endpointName) {
    return (arg, {
      track = true,
      fixedCacheKey
    } = {}) => (dispatch, getState) => {
      const thunk = mutationThunk({
        type: "mutation",
        endpointName,
        originalArgs: arg,
        track,
        fixedCacheKey
      });
      const thunkResult = dispatch(thunk);
      middlewareWarning(dispatch);
      const {
        requestId,
        abort,
        unwrap
      } = thunkResult;
      const returnValuePromise = asSafePromise(thunkResult.unwrap().then((data) => ({
        data
      })), (error) => ({
        error
      }));
      const reset = () => {
        dispatch(removeMutationResult({
          requestId,
          fixedCacheKey
        }));
      };
      const ret = Object.assign(returnValuePromise, {
        arg: thunkResult.arg,
        requestId,
        abort,
        unwrap,
        reset
      });
      const running = runningMutations.get(dispatch) || {};
      runningMutations.set(dispatch, running);
      running[requestId] = ret;
      ret.then(() => {
        delete running[requestId];
        if (!countObjectKeys(running)) {
          runningMutations.delete(dispatch);
        }
      });
      if (fixedCacheKey) {
        running[fixedCacheKey] = ret;
        ret.then(() => {
          if (running[fixedCacheKey] === ret) {
            delete running[fixedCacheKey];
            if (!countObjectKeys(running)) {
              runningMutations.delete(dispatch);
            }
          }
        });
      }
      return ret;
    };
  }
}

// src/query/standardSchema.ts
var import_utils4 = require("@standard-schema/utils");
var NamedSchemaError = class extends import_utils4.SchemaError {
  constructor(issues, value, schemaName, _bqMeta) {
    super(issues);
    this.value = value;
    this.schemaName = schemaName;
    this._bqMeta = _bqMeta;
  }
};
var shouldSkip = (skipSchemaValidation, schemaName) => Array.isArray(skipSchemaValidation) ? skipSchemaValidation.includes(schemaName) : !!skipSchemaValidation;
async function parseWithSchema(schema, data, schemaName, bqMeta) {
  const result = await schema["~standard"].validate(data);
  if (result.issues) {
    throw new NamedSchemaError(result.issues, data, schemaName, bqMeta);
  }
  return result.value;
}

// src/query/core/buildThunks.ts
function defaultTransformResponse(baseQueryReturnValue) {
  return baseQueryReturnValue;
}
var addShouldAutoBatch = (arg = {}) => {
  return {
    ...arg,
    [import_toolkit.SHOULD_AUTOBATCH]: true
  };
};
function buildThunks({
  reducerPath,
  baseQuery,
  context: {
    endpointDefinitions
  },
  serializeQueryArgs,
  api,
  assertTagType,
  selectors,
  onSchemaFailure,
  catchSchemaFailure: globalCatchSchemaFailure,
  skipSchemaValidation: globalSkipSchemaValidation
}) {
  const patchQueryData = (endpointName, arg, patches, updateProvided) => (dispatch, getState) => {
    const endpointDefinition = endpointDefinitions[endpointName];
    const queryCacheKey = serializeQueryArgs({
      queryArgs: arg,
      endpointDefinition,
      endpointName
    });
    dispatch(api.internalActions.queryResultPatched({
      queryCacheKey,
      patches
    }));
    if (!updateProvided) {
      return;
    }
    const newValue = api.endpoints[endpointName].select(arg)(
      // Work around TS 4.1 mismatch
      getState()
    );
    const providedTags = calculateProvidedBy(endpointDefinition.providesTags, newValue.data, void 0, arg, {}, assertTagType);
    dispatch(api.internalActions.updateProvidedBy([{
      queryCacheKey,
      providedTags
    }]));
  };
  function addToStart(items, item, max = 0) {
    const newItems = [item, ...items];
    return max && newItems.length > max ? newItems.slice(0, -1) : newItems;
  }
  function addToEnd(items, item, max = 0) {
    const newItems = [...items, item];
    return max && newItems.length > max ? newItems.slice(1) : newItems;
  }
  const updateQueryData = (endpointName, arg, updateRecipe, updateProvided = true) => (dispatch, getState) => {
    const endpointDefinition = api.endpoints[endpointName];
    const currentState = endpointDefinition.select(arg)(
      // Work around TS 4.1 mismatch
      getState()
    );
    const ret = {
      patches: [],
      inversePatches: [],
      undo: () => dispatch(api.util.patchQueryData(endpointName, arg, ret.inversePatches, updateProvided))
    };
    if (currentState.status === "uninitialized" /* uninitialized */) {
      return ret;
    }
    let newValue;
    if ("data" in currentState) {
      if ((0, import_immer.isDraftable)(currentState.data)) {
        const [value, patches, inversePatches] = (0, import_immer.produceWithPatches)(currentState.data, updateRecipe);
        ret.patches.push(...patches);
        ret.inversePatches.push(...inversePatches);
        newValue = value;
      } else {
        newValue = updateRecipe(currentState.data);
        ret.patches.push({
          op: "replace",
          path: [],
          value: newValue
        });
        ret.inversePatches.push({
          op: "replace",
          path: [],
          value: currentState.data
        });
      }
    }
    if (ret.patches.length === 0) {
      return ret;
    }
    dispatch(api.util.patchQueryData(endpointName, arg, ret.patches, updateProvided));
    return ret;
  };
  const upsertQueryData = (endpointName, arg, value) => (dispatch) => {
    const res = dispatch(api.endpoints[endpointName].initiate(arg, {
      subscribe: false,
      forceRefetch: true,
      [forceQueryFnSymbol]: () => ({
        data: value
      })
    }));
    return res;
  };
  const getTransformCallbackForEndpoint = (endpointDefinition, transformFieldName) => {
    return endpointDefinition.query && endpointDefinition[transformFieldName] ? endpointDefinition[transformFieldName] : defaultTransformResponse;
  };
  const executeEndpoint = async (arg, {
    signal,
    abort,
    rejectWithValue,
    fulfillWithValue,
    dispatch,
    getState,
    extra
  }) => {
    const endpointDefinition = endpointDefinitions[arg.endpointName];
    const {
      metaSchema,
      skipSchemaValidation = globalSkipSchemaValidation
    } = endpointDefinition;
    try {
      let transformResponse = defaultTransformResponse;
      const baseQueryApi = {
        signal,
        abort,
        dispatch,
        getState,
        extra,
        endpoint: arg.endpointName,
        type: arg.type,
        forced: arg.type === "query" ? isForcedQuery(arg, getState()) : void 0,
        queryCacheKey: arg.type === "query" ? arg.queryCacheKey : void 0
      };
      const forceQueryFn = arg.type === "query" ? arg[forceQueryFnSymbol] : void 0;
      let finalQueryReturnValue;
      const fetchPage = async (data, param, maxPages, previous) => {
        if (param == null && data.pages.length) {
          return Promise.resolve({
            data
          });
        }
        const finalQueryArg = {
          queryArg: arg.originalArgs,
          pageParam: param
        };
        const pageResponse = await executeRequest(finalQueryArg);
        const addTo = previous ? addToStart : addToEnd;
        return {
          data: {
            pages: addTo(data.pages, pageResponse.data, maxPages),
            pageParams: addTo(data.pageParams, param, maxPages)
          },
          meta: pageResponse.meta
        };
      };
      async function executeRequest(finalQueryArg) {
        let result;
        const {
          extraOptions,
          argSchema,
          rawResponseSchema,
          responseSchema
        } = endpointDefinition;
        if (argSchema && !shouldSkip(skipSchemaValidation, "arg")) {
          finalQueryArg = await parseWithSchema(
            argSchema,
            finalQueryArg,
            "argSchema",
            {}
            // we don't have a meta yet, so we can't pass it
          );
        }
        if (forceQueryFn) {
          result = forceQueryFn();
        } else if (endpointDefinition.query) {
          transformResponse = getTransformCallbackForEndpoint(endpointDefinition, "transformResponse");
          result = await baseQuery(endpointDefinition.query(finalQueryArg), baseQueryApi, extraOptions);
        } else {
          result = await endpointDefinition.queryFn(finalQueryArg, baseQueryApi, extraOptions, (arg2) => baseQuery(arg2, baseQueryApi, extraOptions));
        }
        if (typeof process !== "undefined" && true) {
          const what = endpointDefinition.query ? "`baseQuery`" : "`queryFn`";
          let err;
          if (!result) {
            err = `${what} did not return anything.`;
          } else if (typeof result !== "object") {
            err = `${what} did not return an object.`;
          } else if (result.error && result.data) {
            err = `${what} returned an object containing both \`error\` and \`result\`.`;
          } else if (result.error === void 0 && result.data === void 0) {
            err = `${what} returned an object containing neither a valid \`error\` and \`result\`. At least one of them should not be \`undefined\``;
          } else {
            for (const key of Object.keys(result)) {
              if (key !== "error" && key !== "data" && key !== "meta") {
                err = `The object returned by ${what} has the unknown property ${key}.`;
                break;
              }
            }
          }
          if (err) {
            console.error(`Error encountered handling the endpoint ${arg.endpointName}.
                  ${err}
                  It needs to return an object with either the shape \`{ data: <value> }\` or \`{ error: <value> }\` that may contain an optional \`meta\` property.
                  Object returned was:`, result);
          }
        }
        if (result.error) throw new HandledError(result.error, result.meta);
        let {
          data
        } = result;
        if (rawResponseSchema && !shouldSkip(skipSchemaValidation, "rawResponse")) {
          data = await parseWithSchema(rawResponseSchema, result.data, "rawResponseSchema", result.meta);
        }
        let transformedResponse = await transformResponse(data, result.meta, finalQueryArg);
        if (responseSchema && !shouldSkip(skipSchemaValidation, "response")) {
          transformedResponse = await parseWithSchema(responseSchema, transformedResponse, "responseSchema", result.meta);
        }
        return {
          ...result,
          data: transformedResponse
        };
      }
      if (arg.type === "query" && "infiniteQueryOptions" in endpointDefinition) {
        const {
          infiniteQueryOptions
        } = endpointDefinition;
        const {
          maxPages = Infinity
        } = infiniteQueryOptions;
        let result;
        const blankData = {
          pages: [],
          pageParams: []
        };
        const cachedData = selectors.selectQueryEntry(getState(), arg.queryCacheKey)?.data;
        const isForcedQueryNeedingRefetch = (
          // arg.forceRefetch
          isForcedQuery(arg, getState()) && !arg.direction
        );
        const existingData = isForcedQueryNeedingRefetch || !cachedData ? blankData : cachedData;
        if ("direction" in arg && arg.direction && existingData.pages.length) {
          const previous = arg.direction === "backward";
          const pageParamFn = previous ? getPreviousPageParam : getNextPageParam;
          const param = pageParamFn(infiniteQueryOptions, existingData, arg.originalArgs);
          result = await fetchPage(existingData, param, maxPages, previous);
        } else {
          const {
            initialPageParam = infiniteQueryOptions.initialPageParam
          } = arg;
          const cachedPageParams = cachedData?.pageParams ?? [];
          const firstPageParam = cachedPageParams[0] ?? initialPageParam;
          const totalPages = cachedPageParams.length;
          result = await fetchPage(existingData, firstPageParam, maxPages);
          if (forceQueryFn) {
            result = {
              data: result.data.pages[0]
            };
          }
          for (let i = 1; i < totalPages; i++) {
            const param = getNextPageParam(infiniteQueryOptions, result.data, arg.originalArgs);
            result = await fetchPage(result.data, param, maxPages);
          }
        }
        finalQueryReturnValue = result;
      } else {
        finalQueryReturnValue = await executeRequest(arg.originalArgs);
      }
      if (metaSchema && !shouldSkip(skipSchemaValidation, "meta") && finalQueryReturnValue.meta) {
        finalQueryReturnValue.meta = await parseWithSchema(metaSchema, finalQueryReturnValue.meta, "metaSchema", finalQueryReturnValue.meta);
      }
      return fulfillWithValue(finalQueryReturnValue.data, addShouldAutoBatch({
        fulfilledTimeStamp: Date.now(),
        baseQueryMeta: finalQueryReturnValue.meta
      }));
    } catch (error) {
      let caughtError = error;
      if (caughtError instanceof HandledError) {
        let transformErrorResponse = getTransformCallbackForEndpoint(endpointDefinition, "transformErrorResponse");
        const {
          rawErrorResponseSchema,
          errorResponseSchema
        } = endpointDefinition;
        let {
          value,
          meta
        } = caughtError;
        try {
          if (rawErrorResponseSchema && !shouldSkip(skipSchemaValidation, "rawErrorResponse")) {
            value = await parseWithSchema(rawErrorResponseSchema, value, "rawErrorResponseSchema", meta);
          }
          if (metaSchema && !shouldSkip(skipSchemaValidation, "meta")) {
            meta = await parseWithSchema(metaSchema, meta, "metaSchema", meta);
          }
          let transformedErrorResponse = await transformErrorResponse(value, meta, arg.originalArgs);
          if (errorResponseSchema && !shouldSkip(skipSchemaValidation, "errorResponse")) {
            transformedErrorResponse = await parseWithSchema(errorResponseSchema, transformedErrorResponse, "errorResponseSchema", meta);
          }
          return rejectWithValue(transformedErrorResponse, addShouldAutoBatch({
            baseQueryMeta: meta
          }));
        } catch (e) {
          caughtError = e;
        }
      }
      try {
        if (caughtError instanceof NamedSchemaError) {
          const info = {
            endpoint: arg.endpointName,
            arg: arg.originalArgs,
            type: arg.type,
            queryCacheKey: arg.type === "query" ? arg.queryCacheKey : void 0
          };
          endpointDefinition.onSchemaFailure?.(caughtError, info);
          onSchemaFailure?.(caughtError, info);
          const {
            catchSchemaFailure = globalCatchSchemaFailure
          } = endpointDefinition;
          if (catchSchemaFailure) {
            return rejectWithValue(catchSchemaFailure(caughtError, info), addShouldAutoBatch({
              baseQueryMeta: caughtError._bqMeta
            }));
          }
        }
      } catch (e) {
        caughtError = e;
      }
      if (typeof process !== "undefined" && true) {
        console.error(`An unhandled error occurred processing a request for the endpoint "${arg.endpointName}".
In the case of an unhandled error, no tags will be "provided" or "invalidated".`, caughtError);
      } else {
        console.error(caughtError);
      }
      throw caughtError;
    }
  };
  function isForcedQuery(arg, state) {
    const requestState = selectors.selectQueryEntry(state, arg.queryCacheKey);
    const baseFetchOnMountOrArgChange = selectors.selectConfig(state).refetchOnMountOrArgChange;
    const fulfilledVal = requestState?.fulfilledTimeStamp;
    const refetchVal = arg.forceRefetch ?? (arg.subscribe && baseFetchOnMountOrArgChange);
    if (refetchVal) {
      return refetchVal === true || (Number(/* @__PURE__ */ new Date()) - Number(fulfilledVal)) / 1e3 >= refetchVal;
    }
    return false;
  }
  const createQueryThunk = () => {
    const generatedQueryThunk = (0, import_toolkit.createAsyncThunk)(`${reducerPath}/executeQuery`, executeEndpoint, {
      getPendingMeta({
        arg
      }) {
        const endpointDefinition = endpointDefinitions[arg.endpointName];
        return addShouldAutoBatch({
          startedTimeStamp: Date.now(),
          ...isInfiniteQueryDefinition(endpointDefinition) ? {
            direction: arg.direction
          } : {}
        });
      },
      condition(queryThunkArg, {
        getState
      }) {
        const state = getState();
        const requestState = selectors.selectQueryEntry(state, queryThunkArg.queryCacheKey);
        const fulfilledVal = requestState?.fulfilledTimeStamp;
        const currentArg = queryThunkArg.originalArgs;
        const previousArg = requestState?.originalArgs;
        const endpointDefinition = endpointDefinitions[queryThunkArg.endpointName];
        const direction = queryThunkArg.direction;
        if (isUpsertQuery(queryThunkArg)) {
          return true;
        }
        if (requestState?.status === "pending") {
          return false;
        }
        if (isForcedQuery(queryThunkArg, state)) {
          return true;
        }
        if (isQueryDefinition(endpointDefinition) && endpointDefinition?.forceRefetch?.({
          currentArg,
          previousArg,
          endpointState: requestState,
          state
        })) {
          return true;
        }
        if (fulfilledVal && !direction) {
          return false;
        }
        return true;
      },
      dispatchConditionRejection: true
    });
    return generatedQueryThunk;
  };
  const queryThunk = createQueryThunk();
  const infiniteQueryThunk = createQueryThunk();
  const mutationThunk = (0, import_toolkit.createAsyncThunk)(`${reducerPath}/executeMutation`, executeEndpoint, {
    getPendingMeta() {
      return addShouldAutoBatch({
        startedTimeStamp: Date.now()
      });
    }
  });
  const hasTheForce = (options) => "force" in options;
  const hasMaxAge = (options) => "ifOlderThan" in options;
  const prefetch = (endpointName, arg, options) => (dispatch, getState) => {
    const force = hasTheForce(options) && options.force;
    const maxAge = hasMaxAge(options) && options.ifOlderThan;
    const queryAction = (force2 = true) => {
      const options2 = {
        forceRefetch: force2,
        isPrefetch: true
      };
      return api.endpoints[endpointName].initiate(arg, options2);
    };
    const latestStateValue = api.endpoints[endpointName].select(arg)(getState());
    if (force) {
      dispatch(queryAction());
    } else if (maxAge) {
      const lastFulfilledTs = latestStateValue?.fulfilledTimeStamp;
      if (!lastFulfilledTs) {
        dispatch(queryAction());
        return;
      }
      const shouldRetrigger = (Number(/* @__PURE__ */ new Date()) - Number(new Date(lastFulfilledTs))) / 1e3 >= maxAge;
      if (shouldRetrigger) {
        dispatch(queryAction());
      }
    } else {
      dispatch(queryAction(false));
    }
  };
  function matchesEndpoint(endpointName) {
    return (action) => action?.meta?.arg?.endpointName === endpointName;
  }
  function buildMatchThunkActions(thunk, endpointName) {
    return {
      matchPending: (0, import_toolkit.isAllOf)((0, import_toolkit.isPending)(thunk), matchesEndpoint(endpointName)),
      matchFulfilled: (0, import_toolkit.isAllOf)((0, import_toolkit.isFulfilled)(thunk), matchesEndpoint(endpointName)),
      matchRejected: (0, import_toolkit.isAllOf)((0, import_toolkit.isRejected)(thunk), matchesEndpoint(endpointName))
    };
  }
  return {
    queryThunk,
    mutationThunk,
    infiniteQueryThunk,
    prefetch,
    updateQueryData,
    upsertQueryData,
    patchQueryData,
    buildMatchThunkActions
  };
}
function getNextPageParam(options, {
  pages,
  pageParams
}, queryArg) {
  const lastIndex = pages.length - 1;
  return options.getNextPageParam(pages[lastIndex], pages, pageParams[lastIndex], pageParams, queryArg);
}
function getPreviousPageParam(options, {
  pages,
  pageParams
}, queryArg) {
  return options.getPreviousPageParam?.(pages[0], pages, pageParams[0], pageParams, queryArg);
}
function calculateProvidedByThunk(action, type, endpointDefinitions, assertTagType) {
  return calculateProvidedBy(endpointDefinitions[action.meta.arg.endpointName][type], (0, import_toolkit.isFulfilled)(action) ? action.payload : void 0, (0, import_toolkit.isRejectedWithValue)(action) ? action.payload : void 0, action.meta.arg.originalArgs, "baseQueryMeta" in action.meta ? action.meta.baseQueryMeta : void 0, assertTagType);
}

// src/query/core/buildSlice.ts
var import_immer2 = require("immer");
var import_immer3 = require("immer");
function updateQuerySubstateIfExists(state, queryCacheKey, update) {
  const substate = state[queryCacheKey];
  if (substate) {
    update(substate);
  }
}
function getMutationCacheKey(id) {
  return ("arg" in id ? id.arg.fixedCacheKey : id.fixedCacheKey) ?? id.requestId;
}
function updateMutationSubstateIfExists(state, id, update) {
  const substate = state[getMutationCacheKey(id)];
  if (substate) {
    update(substate);
  }
}
var initialState = {};
function buildSlice({
  reducerPath,
  queryThunk,
  mutationThunk,
  serializeQueryArgs,
  context: {
    endpointDefinitions: definitions,
    apiUid,
    extractRehydrationInfo,
    hasRehydrationInfo
  },
  assertTagType,
  config
}) {
  const resetApiState = (0, import_toolkit.createAction)(`${reducerPath}/resetApiState`);
  function writePendingCacheEntry(draft, arg, upserting, meta) {
    draft[arg.queryCacheKey] ??= {
      status: "uninitialized" /* uninitialized */,
      endpointName: arg.endpointName
    };
    updateQuerySubstateIfExists(draft, arg.queryCacheKey, (substate) => {
      substate.status = "pending" /* pending */;
      substate.requestId = upserting && substate.requestId ? (
        // for `upsertQuery` **updates**, keep the current `requestId`
        substate.requestId
      ) : (
        // for normal queries or `upsertQuery` **inserts** always update the `requestId`
        meta.requestId
      );
      if (arg.originalArgs !== void 0) {
        substate.originalArgs = arg.originalArgs;
      }
      substate.startedTimeStamp = meta.startedTimeStamp;
      const endpointDefinition = definitions[meta.arg.endpointName];
      if (isInfiniteQueryDefinition(endpointDefinition) && "direction" in arg) {
        ;
        substate.direction = arg.direction;
      }
    });
  }
  function writeFulfilledCacheEntry(draft, meta, payload, upserting) {
    updateQuerySubstateIfExists(draft, meta.arg.queryCacheKey, (substate) => {
      if (substate.requestId !== meta.requestId && !upserting) return;
      const {
        merge
      } = definitions[meta.arg.endpointName];
      substate.status = "fulfilled" /* fulfilled */;
      if (merge) {
        if (substate.data !== void 0) {
          const {
            fulfilledTimeStamp,
            arg,
            baseQueryMeta,
            requestId
          } = meta;
          let newData = (0, import_toolkit.createNextState)(substate.data, (draftSubstateData) => {
            return merge(draftSubstateData, payload, {
              arg: arg.originalArgs,
              baseQueryMeta,
              fulfilledTimeStamp,
              requestId
            });
          });
          substate.data = newData;
        } else {
          substate.data = payload;
        }
      } else {
        substate.data = definitions[meta.arg.endpointName].structuralSharing ?? true ? copyWithStructuralSharing((0, import_immer2.isDraft)(substate.data) ? (0, import_immer3.original)(substate.data) : substate.data, payload) : payload;
      }
      delete substate.error;
      substate.fulfilledTimeStamp = meta.fulfilledTimeStamp;
    });
  }
  const querySlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/queries`,
    initialState,
    reducers: {
      removeQueryResult: {
        reducer(draft, {
          payload: {
            queryCacheKey
          }
        }) {
          delete draft[queryCacheKey];
        },
        prepare: (0, import_toolkit.prepareAutoBatched)()
      },
      cacheEntriesUpserted: {
        reducer(draft, action) {
          for (const entry of action.payload) {
            const {
              queryDescription: arg,
              value
            } = entry;
            writePendingCacheEntry(draft, arg, true, {
              arg,
              requestId: action.meta.requestId,
              startedTimeStamp: action.meta.timestamp
            });
            writeFulfilledCacheEntry(
              draft,
              {
                arg,
                requestId: action.meta.requestId,
                fulfilledTimeStamp: action.meta.timestamp,
                baseQueryMeta: {}
              },
              value,
              // We know we're upserting here
              true
            );
          }
        },
        prepare: (payload) => {
          const queryDescriptions = payload.map((entry) => {
            const {
              endpointName,
              arg,
              value
            } = entry;
            const endpointDefinition = definitions[endpointName];
            const queryDescription = {
              type: "query",
              endpointName,
              originalArgs: entry.arg,
              queryCacheKey: serializeQueryArgs({
                queryArgs: arg,
                endpointDefinition,
                endpointName
              })
            };
            return {
              queryDescription,
              value
            };
          });
          const result = {
            payload: queryDescriptions,
            meta: {
              [import_toolkit.SHOULD_AUTOBATCH]: true,
              requestId: (0, import_toolkit.nanoid)(),
              timestamp: Date.now()
            }
          };
          return result;
        }
      },
      queryResultPatched: {
        reducer(draft, {
          payload: {
            queryCacheKey,
            patches
          }
        }) {
          updateQuerySubstateIfExists(draft, queryCacheKey, (substate) => {
            substate.data = (0, import_immer3.applyPatches)(substate.data, patches.concat());
          });
        },
        prepare: (0, import_toolkit.prepareAutoBatched)()
      }
    },
    extraReducers(builder) {
      builder.addCase(queryThunk.pending, (draft, {
        meta,
        meta: {
          arg
        }
      }) => {
        const upserting = isUpsertQuery(arg);
        writePendingCacheEntry(draft, arg, upserting, meta);
      }).addCase(queryThunk.fulfilled, (draft, {
        meta,
        payload
      }) => {
        const upserting = isUpsertQuery(meta.arg);
        writeFulfilledCacheEntry(draft, meta, payload, upserting);
      }).addCase(queryThunk.rejected, (draft, {
        meta: {
          condition,
          arg,
          requestId
        },
        error,
        payload
      }) => {
        updateQuerySubstateIfExists(draft, arg.queryCacheKey, (substate) => {
          if (condition) {
          } else {
            if (substate.requestId !== requestId) return;
            substate.status = "rejected" /* rejected */;
            substate.error = payload ?? error;
          }
        });
      }).addMatcher(hasRehydrationInfo, (draft, action) => {
        const {
          queries
        } = extractRehydrationInfo(action);
        for (const [key, entry] of Object.entries(queries)) {
          if (
            // do not rehydrate entries that were currently in flight.
            entry?.status === "fulfilled" /* fulfilled */ || entry?.status === "rejected" /* rejected */
          ) {
            draft[key] = entry;
          }
        }
      });
    }
  });
  const mutationSlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/mutations`,
    initialState,
    reducers: {
      removeMutationResult: {
        reducer(draft, {
          payload
        }) {
          const cacheKey = getMutationCacheKey(payload);
          if (cacheKey in draft) {
            delete draft[cacheKey];
          }
        },
        prepare: (0, import_toolkit.prepareAutoBatched)()
      }
    },
    extraReducers(builder) {
      builder.addCase(mutationThunk.pending, (draft, {
        meta,
        meta: {
          requestId,
          arg,
          startedTimeStamp
        }
      }) => {
        if (!arg.track) return;
        draft[getMutationCacheKey(meta)] = {
          requestId,
          status: "pending" /* pending */,
          endpointName: arg.endpointName,
          startedTimeStamp
        };
      }).addCase(mutationThunk.fulfilled, (draft, {
        payload,
        meta
      }) => {
        if (!meta.arg.track) return;
        updateMutationSubstateIfExists(draft, meta, (substate) => {
          if (substate.requestId !== meta.requestId) return;
          substate.status = "fulfilled" /* fulfilled */;
          substate.data = payload;
          substate.fulfilledTimeStamp = meta.fulfilledTimeStamp;
        });
      }).addCase(mutationThunk.rejected, (draft, {
        payload,
        error,
        meta
      }) => {
        if (!meta.arg.track) return;
        updateMutationSubstateIfExists(draft, meta, (substate) => {
          if (substate.requestId !== meta.requestId) return;
          substate.status = "rejected" /* rejected */;
          substate.error = payload ?? error;
        });
      }).addMatcher(hasRehydrationInfo, (draft, action) => {
        const {
          mutations
        } = extractRehydrationInfo(action);
        for (const [key, entry] of Object.entries(mutations)) {
          if (
            // do not rehydrate entries that were currently in flight.
            (entry?.status === "fulfilled" /* fulfilled */ || entry?.status === "rejected" /* rejected */) && // only rehydrate endpoints that were persisted using a `fixedCacheKey`
            key !== entry?.requestId
          ) {
            draft[key] = entry;
          }
        }
      });
    }
  });
  const initialInvalidationState = {
    tags: {},
    keys: {}
  };
  const invalidationSlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/invalidation`,
    initialState: initialInvalidationState,
    reducers: {
      updateProvidedBy: {
        reducer(draft, action) {
          for (const {
            queryCacheKey,
            providedTags
          } of action.payload) {
            removeCacheKeyFromTags(draft, queryCacheKey);
            for (const {
              type,
              id
            } of providedTags) {
              const subscribedQueries = (draft.tags[type] ??= {})[id || "__internal_without_id"] ??= [];
              const alreadySubscribed = subscribedQueries.includes(queryCacheKey);
              if (!alreadySubscribed) {
                subscribedQueries.push(queryCacheKey);
              }
            }
            draft.keys[queryCacheKey] = providedTags;
          }
        },
        prepare: (0, import_toolkit.prepareAutoBatched)()
      }
    },
    extraReducers(builder) {
      builder.addCase(querySlice.actions.removeQueryResult, (draft, {
        payload: {
          queryCacheKey
        }
      }) => {
        removeCacheKeyFromTags(draft, queryCacheKey);
      }).addMatcher(hasRehydrationInfo, (draft, action) => {
        const {
          provided
        } = extractRehydrationInfo(action);
        for (const [type, incomingTags] of Object.entries(provided.tags ?? {})) {
          for (const [id, cacheKeys] of Object.entries(incomingTags)) {
            const subscribedQueries = (draft.tags[type] ??= {})[id || "__internal_without_id"] ??= [];
            for (const queryCacheKey of cacheKeys) {
              const alreadySubscribed = subscribedQueries.includes(queryCacheKey);
              if (!alreadySubscribed) {
                subscribedQueries.push(queryCacheKey);
              }
              draft.keys[queryCacheKey] = provided.keys[queryCacheKey];
            }
          }
        }
      }).addMatcher((0, import_toolkit.isAnyOf)((0, import_toolkit.isFulfilled)(queryThunk), (0, import_toolkit.isRejectedWithValue)(queryThunk)), (draft, action) => {
        writeProvidedTagsForQueries(draft, [action]);
      }).addMatcher(querySlice.actions.cacheEntriesUpserted.match, (draft, action) => {
        const mockActions = action.payload.map(({
          queryDescription,
          value
        }) => {
          return {
            type: "UNKNOWN",
            payload: value,
            meta: {
              requestStatus: "fulfilled",
              requestId: "UNKNOWN",
              arg: queryDescription
            }
          };
        });
        writeProvidedTagsForQueries(draft, mockActions);
      });
    }
  });
  function removeCacheKeyFromTags(draft, queryCacheKey) {
    const existingTags = draft.keys[queryCacheKey] ?? [];
    for (const tag of existingTags) {
      const tagType = tag.type;
      const tagId = tag.id ?? "__internal_without_id";
      const tagSubscriptions = draft.tags[tagType]?.[tagId];
      if (tagSubscriptions) {
        draft.tags[tagType][tagId] = tagSubscriptions.filter((qc) => qc !== queryCacheKey);
      }
    }
    delete draft.keys[queryCacheKey];
  }
  function writeProvidedTagsForQueries(draft, actions2) {
    const providedByEntries = actions2.map((action) => {
      const providedTags = calculateProvidedByThunk(action, "providesTags", definitions, assertTagType);
      const {
        queryCacheKey
      } = action.meta.arg;
      return {
        queryCacheKey,
        providedTags
      };
    });
    invalidationSlice.caseReducers.updateProvidedBy(draft, invalidationSlice.actions.updateProvidedBy(providedByEntries));
  }
  const subscriptionSlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/subscriptions`,
    initialState,
    reducers: {
      updateSubscriptionOptions(d, a) {
      },
      unsubscribeQueryResult(d, a) {
      },
      internal_getRTKQSubscriptions() {
      }
    }
  });
  const internalSubscriptionsSlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/internalSubscriptions`,
    initialState,
    reducers: {
      subscriptionsUpdated: {
        reducer(state, action) {
          return (0, import_immer3.applyPatches)(state, action.payload);
        },
        prepare: (0, import_toolkit.prepareAutoBatched)()
      }
    }
  });
  const configSlice = (0, import_toolkit.createSlice)({
    name: `${reducerPath}/config`,
    initialState: {
      online: isOnline(),
      focused: isDocumentVisible(),
      middlewareRegistered: false,
      ...config
    },
    reducers: {
      middlewareRegistered(state, {
        payload
      }) {
        state.middlewareRegistered = state.middlewareRegistered === "conflict" || apiUid !== payload ? "conflict" : true;
      }
    },
    extraReducers: (builder) => {
      builder.addCase(onOnline, (state) => {
        state.online = true;
      }).addCase(onOffline, (state) => {
        state.online = false;
      }).addCase(onFocus, (state) => {
        state.focused = true;
      }).addCase(onFocusLost, (state) => {
        state.focused = false;
      }).addMatcher(hasRehydrationInfo, (draft) => ({
        ...draft
      }));
    }
  });
  const combinedReducer = (0, import_toolkit.combineReducers)({
    queries: querySlice.reducer,
    mutations: mutationSlice.reducer,
    provided: invalidationSlice.reducer,
    subscriptions: internalSubscriptionsSlice.reducer,
    config: configSlice.reducer
  });
  const reducer = (state, action) => combinedReducer(resetApiState.match(action) ? void 0 : state, action);
  const actions = {
    ...configSlice.actions,
    ...querySlice.actions,
    ...subscriptionSlice.actions,
    ...internalSubscriptionsSlice.actions,
    ...mutationSlice.actions,
    ...invalidationSlice.actions,
    resetApiState
  };
  return {
    reducer,
    actions
  };
}

// src/query/core/buildSelectors.ts
var skipToken = /* @__PURE__ */ Symbol.for("RTKQ/skipToken");
var initialSubState = {
  status: "uninitialized" /* uninitialized */
};
var defaultQuerySubState = /* @__PURE__ */ (0, import_toolkit.createNextState)(initialSubState, () => {
});
var defaultMutationSubState = /* @__PURE__ */ (0, import_toolkit.createNextState)(initialSubState, () => {
});
function buildSelectors({
  serializeQueryArgs,
  reducerPath,
  createSelector: createSelector2
}) {
  const selectSkippedQuery = (state) => defaultQuerySubState;
  const selectSkippedMutation = (state) => defaultMutationSubState;
  return {
    buildQuerySelector,
    buildInfiniteQuerySelector,
    buildMutationSelector,
    selectInvalidatedBy,
    selectCachedArgsForQuery,
    selectApiState,
    selectQueries,
    selectMutations,
    selectQueryEntry,
    selectConfig
  };
  function withRequestFlags(substate) {
    return {
      ...substate,
      ...getRequestStatusFlags(substate.status)
    };
  }
  function selectApiState(rootState) {
    const state = rootState[reducerPath];
    if (true) {
      if (!state) {
        if (selectApiState.triggered) return state;
        selectApiState.triggered = true;
        console.error(`Error: No data found at \`state.${reducerPath}\`. Did you forget to add the reducer to the store?`);
      }
    }
    return state;
  }
  function selectQueries(rootState) {
    return selectApiState(rootState)?.queries;
  }
  function selectQueryEntry(rootState, cacheKey) {
    return selectQueries(rootState)?.[cacheKey];
  }
  function selectMutations(rootState) {
    return selectApiState(rootState)?.mutations;
  }
  function selectConfig(rootState) {
    return selectApiState(rootState)?.config;
  }
  function buildAnyQuerySelector(endpointName, endpointDefinition, combiner) {
    return (queryArgs) => {
      if (queryArgs === skipToken) {
        return createSelector2(selectSkippedQuery, combiner);
      }
      const serializedArgs = serializeQueryArgs({
        queryArgs,
        endpointDefinition,
        endpointName
      });
      const selectQuerySubstate = (state) => selectQueryEntry(state, serializedArgs) ?? defaultQuerySubState;
      return createSelector2(selectQuerySubstate, combiner);
    };
  }
  function buildQuerySelector(endpointName, endpointDefinition) {
    return buildAnyQuerySelector(endpointName, endpointDefinition, withRequestFlags);
  }
  function buildInfiniteQuerySelector(endpointName, endpointDefinition) {
    const {
      infiniteQueryOptions
    } = endpointDefinition;
    function withInfiniteQueryResultFlags(substate) {
      const stateWithRequestFlags = {
        ...substate,
        ...getRequestStatusFlags(substate.status)
      };
      const {
        isLoading,
        isError,
        direction
      } = stateWithRequestFlags;
      const isForward = direction === "forward";
      const isBackward = direction === "backward";
      return {
        ...stateWithRequestFlags,
        hasNextPage: getHasNextPage(infiniteQueryOptions, stateWithRequestFlags.data, stateWithRequestFlags.originalArgs),
        hasPreviousPage: getHasPreviousPage(infiniteQueryOptions, stateWithRequestFlags.data, stateWithRequestFlags.originalArgs),
        isFetchingNextPage: isLoading && isForward,
        isFetchingPreviousPage: isLoading && isBackward,
        isFetchNextPageError: isError && isForward,
        isFetchPreviousPageError: isError && isBackward
      };
    }
    return buildAnyQuerySelector(endpointName, endpointDefinition, withInfiniteQueryResultFlags);
  }
  function buildMutationSelector() {
    return (id) => {
      let mutationId;
      if (typeof id === "object") {
        mutationId = getMutationCacheKey(id) ?? skipToken;
      } else {
        mutationId = id;
      }
      const selectMutationSubstate = (state) => selectApiState(state)?.mutations?.[mutationId] ?? defaultMutationSubState;
      const finalSelectMutationSubstate = mutationId === skipToken ? selectSkippedMutation : selectMutationSubstate;
      return createSelector2(finalSelectMutationSubstate, withRequestFlags);
    };
  }
  function selectInvalidatedBy(state, tags) {
    const apiState = state[reducerPath];
    const toInvalidate = /* @__PURE__ */ new Set();
    for (const tag of tags.filter(isNotNullish).map(expandTagDescription)) {
      const provided = apiState.provided.tags[tag.type];
      if (!provided) {
        continue;
      }
      let invalidateSubscriptions = (tag.id !== void 0 ? (
        // id given: invalidate all queries that provide this type & id
        provided[tag.id]
      ) : (
        // no id: invalidate all queries that provide this type
        flatten(Object.values(provided))
      )) ?? [];
      for (const invalidate of invalidateSubscriptions) {
        toInvalidate.add(invalidate);
      }
    }
    return flatten(Array.from(toInvalidate.values()).map((queryCacheKey) => {
      const querySubState = apiState.queries[queryCacheKey];
      return querySubState ? [{
        queryCacheKey,
        endpointName: querySubState.endpointName,
        originalArgs: querySubState.originalArgs
      }] : [];
    }));
  }
  function selectCachedArgsForQuery(state, queryName) {
    return Object.values(selectQueries(state)).filter((entry) => entry?.endpointName === queryName && entry.status !== "uninitialized" /* uninitialized */).map((entry) => entry.originalArgs);
  }
  function getHasNextPage(options, data, queryArg) {
    if (!data) return false;
    return getNextPageParam(options, data, queryArg) != null;
  }
  function getHasPreviousPage(options, data, queryArg) {
    if (!data || !options.getPreviousPageParam) return false;
    return getPreviousPageParam(options, data, queryArg) != null;
  }
}

// src/query/createApi.ts
var import_toolkit3 = require("@reduxjs/toolkit");

// src/query/defaultSerializeQueryArgs.ts
var cache = WeakMap ? /* @__PURE__ */ new WeakMap() : void 0;
var defaultSerializeQueryArgs = ({
  endpointName,
  queryArgs
}) => {
  let serialized = "";
  const cached = cache?.get(queryArgs);
  if (typeof cached === "string") {
    serialized = cached;
  } else {
    const stringified = JSON.stringify(queryArgs, (key, value) => {
      value = typeof value === "bigint" ? {
        $bigint: value.toString()
      } : value;
      value = (0, import_toolkit.isPlainObject)(value) ? Object.keys(value).sort().reduce((acc, key2) => {
        acc[key2] = value[key2];
        return acc;
      }, {}) : value;
      return value;
    });
    if ((0, import_toolkit.isPlainObject)(queryArgs)) {
      cache?.set(queryArgs, stringified);
    }
    serialized = stringified;
  }
  return `${endpointName}(${serialized})`;
};

// src/query/createApi.ts
var import_reselect = require("reselect");
function buildCreateApi(...modules) {
  return function baseCreateApi(options) {
    const extractRehydrationInfo = (0, import_reselect.weakMapMemoize)((action) => options.extractRehydrationInfo?.(action, {
      reducerPath: options.reducerPath ?? "api"
    }));
    const optionsWithDefaults = {
      reducerPath: "api",
      keepUnusedDataFor: 60,
      refetchOnMountOrArgChange: false,
      refetchOnFocus: false,
      refetchOnReconnect: false,
      invalidationBehavior: "delayed",
      ...options,
      extractRehydrationInfo,
      serializeQueryArgs(queryArgsApi) {
        let finalSerializeQueryArgs = defaultSerializeQueryArgs;
        if ("serializeQueryArgs" in queryArgsApi.endpointDefinition) {
          const endpointSQA = queryArgsApi.endpointDefinition.serializeQueryArgs;
          finalSerializeQueryArgs = (queryArgsApi2) => {
            const initialResult = endpointSQA(queryArgsApi2);
            if (typeof initialResult === "string") {
              return initialResult;
            } else {
              return defaultSerializeQueryArgs({
                ...queryArgsApi2,
                queryArgs: initialResult
              });
            }
          };
        } else if (options.serializeQueryArgs) {
          finalSerializeQueryArgs = options.serializeQueryArgs;
        }
        return finalSerializeQueryArgs(queryArgsApi);
      },
      tagTypes: [...options.tagTypes || []]
    };
    const context = {
      endpointDefinitions: {},
      batch(fn) {
        fn();
      },
      apiUid: (0, import_toolkit.nanoid)(),
      extractRehydrationInfo,
      hasRehydrationInfo: (0, import_reselect.weakMapMemoize)((action) => extractRehydrationInfo(action) != null)
    };
    const api = {
      injectEndpoints,
      enhanceEndpoints({
        addTagTypes,
        endpoints
      }) {
        if (addTagTypes) {
          for (const eT of addTagTypes) {
            if (!optionsWithDefaults.tagTypes.includes(eT)) {
              ;
              optionsWithDefaults.tagTypes.push(eT);
            }
          }
        }
        if (endpoints) {
          for (const [endpointName, partialDefinition] of Object.entries(endpoints)) {
            if (typeof partialDefinition === "function") {
              partialDefinition(context.endpointDefinitions[endpointName]);
            } else {
              Object.assign(context.endpointDefinitions[endpointName] || {}, partialDefinition);
            }
          }
        }
        return api;
      }
    };
    const initializedModules = modules.map((m) => m.init(api, optionsWithDefaults, context));
    function injectEndpoints(inject) {
      const evaluatedEndpoints = inject.endpoints({
        query: (x) => ({
          ...x,
          type: "query" /* query */
        }),
        mutation: (x) => ({
          ...x,
          type: "mutation" /* mutation */
        }),
        infiniteQuery: (x) => ({
          ...x,
          type: "infinitequery" /* infinitequery */
        })
      });
      for (const [endpointName, definition] of Object.entries(evaluatedEndpoints)) {
        if (inject.overrideExisting !== true && endpointName in context.endpointDefinitions) {
          if (inject.overrideExisting === "throw") {
            throw new Error(false ? _formatProdErrorMessage2(39) : `called \`injectEndpoints\` to override already-existing endpointName ${endpointName} without specifying \`overrideExisting: true\``);
          } else if (typeof process !== "undefined" && true) {
            console.error(`called \`injectEndpoints\` to override already-existing endpointName ${endpointName} without specifying \`overrideExisting: true\``);
          }
          continue;
        }
        if (typeof process !== "undefined" && true) {
          if (isInfiniteQueryDefinition(definition)) {
            const {
              infiniteQueryOptions
            } = definition;
            const {
              maxPages,
              getPreviousPageParam: getPreviousPageParam2
            } = infiniteQueryOptions;
            if (typeof maxPages === "number") {
              if (maxPages < 1) {
                throw new Error(false ? _formatProdErrorMessage22(40) : `maxPages for endpoint '${endpointName}' must be a number greater than 0`);
              }
              if (typeof getPreviousPageParam2 !== "function") {
                throw new Error(false ? _formatProdErrorMessage3(41) : `getPreviousPageParam for endpoint '${endpointName}' must be a function if maxPages is used`);
              }
            }
          }
        }
        context.endpointDefinitions[endpointName] = definition;
        for (const m of initializedModules) {
          m.injectEndpoint(endpointName, definition);
        }
      }
      return api;
    }
    return api.injectEndpoints({
      endpoints: options.endpoints
    });
  };
}

// src/query/fakeBaseQuery.ts
var import_toolkit4 = require("@reduxjs/toolkit");
var _NEVER = /* @__PURE__ */ Symbol();
function fakeBaseQuery() {
  return function() {
    throw new Error(false ? _formatProdErrorMessage4(33) : "When using `fakeBaseQuery`, all queries & mutations must use the `queryFn` definition syntax.");
  };
}

// src/query/core/module.ts
var import_immer5 = require("immer");

// src/query/tsHelpers.ts
function assertCast(v) {
}
function safeAssign(target, ...args) {
  return Object.assign(target, ...args);
}

// src/query/core/buildMiddleware/batchActions.ts
var import_immer4 = require("immer");
var buildBatchedActionsHandler = ({
  api,
  queryThunk,
  internalState,
  mwApi
}) => {
  const subscriptionsPrefix = `${api.reducerPath}/subscriptions`;
  let previousSubscriptions = null;
  let updateSyncTimer = null;
  const {
    updateSubscriptionOptions,
    unsubscribeQueryResult
  } = api.internalActions;
  const actuallyMutateSubscriptions = (currentSubscriptions, action) => {
    if (updateSubscriptionOptions.match(action)) {
      const {
        queryCacheKey,
        requestId,
        options
      } = action.payload;
      const sub = currentSubscriptions.get(queryCacheKey);
      if (sub?.has(requestId)) {
        sub.set(requestId, options);
      }
      return true;
    }
    if (unsubscribeQueryResult.match(action)) {
      const {
        queryCacheKey,
        requestId
      } = action.payload;
      const sub = currentSubscriptions.get(queryCacheKey);
      if (sub) {
        sub.delete(requestId);
      }
      return true;
    }
    if (api.internalActions.removeQueryResult.match(action)) {
      currentSubscriptions.delete(action.payload.queryCacheKey);
      return true;
    }
    if (queryThunk.pending.match(action)) {
      const {
        meta: {
          arg,
          requestId
        }
      } = action;
      const substate = getOrInsertComputed(currentSubscriptions, arg.queryCacheKey, createNewMap);
      if (arg.subscribe) {
        substate.set(requestId, arg.subscriptionOptions ?? substate.get(requestId) ?? {});
      }
      return true;
    }
    let mutated = false;
    if (queryThunk.rejected.match(action)) {
      const {
        meta: {
          condition,
          arg,
          requestId
        }
      } = action;
      if (condition && arg.subscribe) {
        const substate = getOrInsertComputed(currentSubscriptions, arg.queryCacheKey, createNewMap);
        substate.set(requestId, arg.subscriptionOptions ?? substate.get(requestId) ?? {});
        mutated = true;
      }
    }
    return mutated;
  };
  const getSubscriptions = () => internalState.currentSubscriptions;
  const getSubscriptionCount = (queryCacheKey) => {
    const subscriptions = getSubscriptions();
    const subscriptionsForQueryArg = subscriptions.get(queryCacheKey);
    return subscriptionsForQueryArg?.size ?? 0;
  };
  const isRequestSubscribed = (queryCacheKey, requestId) => {
    const subscriptions = getSubscriptions();
    return !!subscriptions?.get(queryCacheKey)?.get(requestId);
  };
  const subscriptionSelectors = {
    getSubscriptions,
    getSubscriptionCount,
    isRequestSubscribed
  };
  function serializeSubscriptions(currentSubscriptions) {
    return JSON.parse(JSON.stringify(Object.fromEntries([...currentSubscriptions].map(([k, v]) => [k, Object.fromEntries(v)]))));
  }
  return (action, mwApi2) => {
    if (!previousSubscriptions) {
      previousSubscriptions = serializeSubscriptions(internalState.currentSubscriptions);
    }
    if (api.util.resetApiState.match(action)) {
      previousSubscriptions = {};
      internalState.currentSubscriptions.clear();
      updateSyncTimer = null;
      return [true, false];
    }
    if (api.internalActions.internal_getRTKQSubscriptions.match(action)) {
      return [false, subscriptionSelectors];
    }
    const didMutate = actuallyMutateSubscriptions(internalState.currentSubscriptions, action);
    let actionShouldContinue = true;
    if (false) {
      return [false, internalState.currentPolls];
    }
    if (didMutate) {
      if (!updateSyncTimer) {
        updateSyncTimer = setTimeout(() => {
          const newSubscriptions = serializeSubscriptions(internalState.currentSubscriptions);
          const [, patches] = (0, import_immer4.produceWithPatches)(previousSubscriptions, () => newSubscriptions);
          mwApi2.next(api.internalActions.subscriptionsUpdated(patches));
          previousSubscriptions = newSubscriptions;
          updateSyncTimer = null;
        }, 500);
      }
      const isSubscriptionSliceAction = typeof action.type == "string" && !!action.type.startsWith(subscriptionsPrefix);
      const isAdditionalSubscriptionAction = queryThunk.rejected.match(action) && action.meta.condition && !!action.meta.arg.subscribe;
      actionShouldContinue = !isSubscriptionSliceAction && !isAdditionalSubscriptionAction;
    }
    return [actionShouldContinue, false];
  };
};

// src/query/core/buildMiddleware/cacheCollection.ts
var THIRTY_TWO_BIT_MAX_TIMER_SECONDS = 2147483647 / 1e3 - 1;
var buildCacheCollectionHandler = ({
  reducerPath,
  api,
  queryThunk,
  context,
  internalState,
  selectors: {
    selectQueryEntry,
    selectConfig
  },
  getRunningQueryThunk,
  mwApi
}) => {
  const {
    removeQueryResult,
    unsubscribeQueryResult,
    cacheEntriesUpserted
  } = api.internalActions;
  const runningQueries = internalState.runningQueries.get(mwApi.dispatch);
  const canTriggerUnsubscribe = (0, import_toolkit.isAnyOf)(unsubscribeQueryResult.match, queryThunk.fulfilled, queryThunk.rejected, cacheEntriesUpserted.match);
  function anySubscriptionsRemainingForKey(queryCacheKey) {
    const subscriptions = internalState.currentSubscriptions.get(queryCacheKey);
    if (!subscriptions) {
      return false;
    }
    const hasSubscriptions = subscriptions.size > 0;
    const isRunning = runningQueries?.[queryCacheKey] !== void 0;
    return hasSubscriptions || isRunning;
  }
  const currentRemovalTimeouts = {};
  const handler = (action, mwApi2, internalState2) => {
    const state = mwApi2.getState();
    const config = selectConfig(state);
    if (canTriggerUnsubscribe(action)) {
      let queryCacheKeys;
      if (cacheEntriesUpserted.match(action)) {
        queryCacheKeys = action.payload.map((entry) => entry.queryDescription.queryCacheKey);
      } else {
        const {
          queryCacheKey
        } = unsubscribeQueryResult.match(action) ? action.payload : action.meta.arg;
        queryCacheKeys = [queryCacheKey];
      }
      handleUnsubscribeMany(queryCacheKeys, mwApi2, config);
    }
    if (api.util.resetApiState.match(action)) {
      for (const [key, timeout] of Object.entries(currentRemovalTimeouts)) {
        if (timeout) clearTimeout(timeout);
        delete currentRemovalTimeouts[key];
      }
    }
    if (context.hasRehydrationInfo(action)) {
      const {
        queries
      } = context.extractRehydrationInfo(action);
      handleUnsubscribeMany(Object.keys(queries), mwApi2, config);
    }
  };
  function handleUnsubscribeMany(cacheKeys, api2, config) {
    const state = api2.getState();
    for (const queryCacheKey of cacheKeys) {
      const entry = selectQueryEntry(state, queryCacheKey);
      if (entry?.endpointName) {
        handleUnsubscribe(queryCacheKey, entry.endpointName, api2, config);
      }
    }
  }
  function handleUnsubscribe(queryCacheKey, endpointName, api2, config) {
    const endpointDefinition = context.endpointDefinitions[endpointName];
    const keepUnusedDataFor = endpointDefinition?.keepUnusedDataFor ?? config.keepUnusedDataFor;
    if (keepUnusedDataFor === Infinity) {
      return;
    }
    const finalKeepUnusedDataFor = Math.max(0, Math.min(keepUnusedDataFor, THIRTY_TWO_BIT_MAX_TIMER_SECONDS));
    if (!anySubscriptionsRemainingForKey(queryCacheKey)) {
      const currentTimeout = currentRemovalTimeouts[queryCacheKey];
      if (currentTimeout) {
        clearTimeout(currentTimeout);
      }
      currentRemovalTimeouts[queryCacheKey] = setTimeout(() => {
        if (!anySubscriptionsRemainingForKey(queryCacheKey)) {
          const entry = selectQueryEntry(api2.getState(), queryCacheKey);
          if (entry?.endpointName) {
            const runningQuery = api2.dispatch(getRunningQueryThunk(entry.endpointName, entry.originalArgs));
            runningQuery?.abort();
          }
          api2.dispatch(removeQueryResult({
            queryCacheKey
          }));
        }
        delete currentRemovalTimeouts[queryCacheKey];
      }, finalKeepUnusedDataFor * 1e3);
    }
  }
  return handler;
};

// src/query/core/buildMiddleware/cacheLifecycle.ts
var neverResolvedError = new Error("Promise never resolved before cacheEntryRemoved.");
var buildCacheLifecycleHandler = ({
  api,
  reducerPath,
  context,
  queryThunk,
  mutationThunk,
  internalState,
  selectors: {
    selectQueryEntry,
    selectApiState
  }
}) => {
  const isQueryThunk = (0, import_toolkit.isAsyncThunkAction)(queryThunk);
  const isMutationThunk = (0, import_toolkit.isAsyncThunkAction)(mutationThunk);
  const isFulfilledThunk = (0, import_toolkit.isFulfilled)(queryThunk, mutationThunk);
  const lifecycleMap = {};
  function resolveLifecycleEntry(cacheKey, data, meta) {
    const lifecycle = lifecycleMap[cacheKey];
    if (lifecycle?.valueResolved) {
      lifecycle.valueResolved({
        data,
        meta
      });
      delete lifecycle.valueResolved;
    }
  }
  function removeLifecycleEntry(cacheKey) {
    const lifecycle = lifecycleMap[cacheKey];
    if (lifecycle) {
      delete lifecycleMap[cacheKey];
      lifecycle.cacheEntryRemoved();
    }
  }
  const handler = (action, mwApi, stateBefore) => {
    const cacheKey = getCacheKey(action);
    function checkForNewCacheKey(endpointName, cacheKey2, requestId, originalArgs) {
      const oldEntry = selectQueryEntry(stateBefore, cacheKey2);
      const newEntry = selectQueryEntry(mwApi.getState(), cacheKey2);
      if (!oldEntry && newEntry) {
        handleNewKey(endpointName, originalArgs, cacheKey2, mwApi, requestId);
      }
    }
    if (queryThunk.pending.match(action)) {
      checkForNewCacheKey(action.meta.arg.endpointName, cacheKey, action.meta.requestId, action.meta.arg.originalArgs);
    } else if (api.internalActions.cacheEntriesUpserted.match(action)) {
      for (const {
        queryDescription,
        value
      } of action.payload) {
        const {
          endpointName,
          originalArgs,
          queryCacheKey
        } = queryDescription;
        checkForNewCacheKey(endpointName, queryCacheKey, action.meta.requestId, originalArgs);
        resolveLifecycleEntry(queryCacheKey, value, {});
      }
    } else if (mutationThunk.pending.match(action)) {
      const state = mwApi.getState()[reducerPath].mutations[cacheKey];
      if (state) {
        handleNewKey(action.meta.arg.endpointName, action.meta.arg.originalArgs, cacheKey, mwApi, action.meta.requestId);
      }
    } else if (isFulfilledThunk(action)) {
      resolveLifecycleEntry(cacheKey, action.payload, action.meta.baseQueryMeta);
    } else if (api.internalActions.removeQueryResult.match(action) || api.internalActions.removeMutationResult.match(action)) {
      removeLifecycleEntry(cacheKey);
    } else if (api.util.resetApiState.match(action)) {
      for (const cacheKey2 of Object.keys(lifecycleMap)) {
        removeLifecycleEntry(cacheKey2);
      }
    }
  };
  function getCacheKey(action) {
    if (isQueryThunk(action)) return action.meta.arg.queryCacheKey;
    if (isMutationThunk(action)) {
      return action.meta.arg.fixedCacheKey ?? action.meta.requestId;
    }
    if (api.internalActions.removeQueryResult.match(action)) return action.payload.queryCacheKey;
    if (api.internalActions.removeMutationResult.match(action)) return getMutationCacheKey(action.payload);
    return "";
  }
  function handleNewKey(endpointName, originalArgs, queryCacheKey, mwApi, requestId) {
    const endpointDefinition = context.endpointDefinitions[endpointName];
    const onCacheEntryAdded = endpointDefinition?.onCacheEntryAdded;
    if (!onCacheEntryAdded) return;
    const lifecycle = {};
    const cacheEntryRemoved = new Promise((resolve) => {
      lifecycle.cacheEntryRemoved = resolve;
    });
    const cacheDataLoaded = Promise.race([new Promise((resolve) => {
      lifecycle.valueResolved = resolve;
    }), cacheEntryRemoved.then(() => {
      throw neverResolvedError;
    })]);
    cacheDataLoaded.catch(() => {
    });
    lifecycleMap[queryCacheKey] = lifecycle;
    const selector = api.endpoints[endpointName].select(isAnyQueryDefinition(endpointDefinition) ? originalArgs : queryCacheKey);
    const extra = mwApi.dispatch((_, __, extra2) => extra2);
    const lifecycleApi = {
      ...mwApi,
      getCacheEntry: () => selector(mwApi.getState()),
      requestId,
      extra,
      updateCachedData: isAnyQueryDefinition(endpointDefinition) ? (updateRecipe) => mwApi.dispatch(api.util.updateQueryData(endpointName, originalArgs, updateRecipe)) : void 0,
      cacheDataLoaded,
      cacheEntryRemoved
    };
    const runningHandler = onCacheEntryAdded(originalArgs, lifecycleApi);
    Promise.resolve(runningHandler).catch((e) => {
      if (e === neverResolvedError) return;
      throw e;
    });
  }
  return handler;
};

// src/query/core/buildMiddleware/devMiddleware.ts
var buildDevCheckHandler = ({
  api,
  context: {
    apiUid
  },
  reducerPath
}) => {
  return (action, mwApi) => {
    if (api.util.resetApiState.match(action)) {
      mwApi.dispatch(api.internalActions.middlewareRegistered(apiUid));
    }
    if (typeof process !== "undefined" && true) {
      if (api.internalActions.middlewareRegistered.match(action) && action.payload === apiUid && mwApi.getState()[reducerPath]?.config?.middlewareRegistered === "conflict") {
        console.warn(`There is a mismatch between slice and middleware for the reducerPath "${reducerPath}".
You can only have one api per reducer path, this will lead to crashes in various situations!${reducerPath === "api" ? `
If you have multiple apis, you *have* to specify the reducerPath option when using createApi!` : ""}`);
      }
    }
  };
};

// src/query/core/buildMiddleware/invalidationByTags.ts
var buildInvalidationByTagsHandler = ({
  reducerPath,
  context,
  context: {
    endpointDefinitions
  },
  mutationThunk,
  queryThunk,
  api,
  assertTagType,
  refetchQuery,
  internalState
}) => {
  const {
    removeQueryResult
  } = api.internalActions;
  const isThunkActionWithTags = (0, import_toolkit.isAnyOf)((0, import_toolkit.isFulfilled)(mutationThunk), (0, import_toolkit.isRejectedWithValue)(mutationThunk));
  const isQueryEnd = (0, import_toolkit.isAnyOf)((0, import_toolkit.isFulfilled)(mutationThunk, queryThunk), (0, import_toolkit.isRejected)(mutationThunk, queryThunk));
  let pendingTagInvalidations = [];
  const handler = (action, mwApi) => {
    if (isThunkActionWithTags(action)) {
      invalidateTags(calculateProvidedByThunk(action, "invalidatesTags", endpointDefinitions, assertTagType), mwApi);
    } else if (isQueryEnd(action)) {
      invalidateTags([], mwApi);
    } else if (api.util.invalidateTags.match(action)) {
      invalidateTags(calculateProvidedBy(action.payload, void 0, void 0, void 0, void 0, assertTagType), mwApi);
    }
  };
  function hasPendingRequests(state) {
    const {
      queries,
      mutations
    } = state;
    for (const cacheRecord of [queries, mutations]) {
      for (const key in cacheRecord) {
        if (cacheRecord[key]?.status === "pending" /* pending */) return true;
      }
    }
    return false;
  }
  function invalidateTags(newTags, mwApi) {
    const rootState = mwApi.getState();
    const state = rootState[reducerPath];
    pendingTagInvalidations.push(...newTags);
    if (state.config.invalidationBehavior === "delayed" && hasPendingRequests(state)) {
      return;
    }
    const tags = pendingTagInvalidations;
    pendingTagInvalidations = [];
    if (tags.length === 0) return;
    const toInvalidate = api.util.selectInvalidatedBy(rootState, tags);
    context.batch(() => {
      const valuesArray = Array.from(toInvalidate.values());
      for (const {
        queryCacheKey
      } of valuesArray) {
        const querySubState = state.queries[queryCacheKey];
        const subscriptionSubState = getOrInsertComputed(internalState.currentSubscriptions, queryCacheKey, createNewMap);
        if (querySubState) {
          if (subscriptionSubState.size === 0) {
            mwApi.dispatch(removeQueryResult({
              queryCacheKey
            }));
          } else if (querySubState.status !== "uninitialized" /* uninitialized */) {
            mwApi.dispatch(refetchQuery(querySubState));
          }
        }
      }
    });
  }
  return handler;
};

// src/query/core/buildMiddleware/polling.ts
var buildPollingHandler = ({
  reducerPath,
  queryThunk,
  api,
  refetchQuery,
  internalState
}) => {
  const {
    currentPolls,
    currentSubscriptions
  } = internalState;
  const pendingPollingUpdates = /* @__PURE__ */ new Set();
  let pollingUpdateTimer = null;
  const handler = (action, mwApi) => {
    if (api.internalActions.updateSubscriptionOptions.match(action) || api.internalActions.unsubscribeQueryResult.match(action)) {
      schedulePollingUpdate(action.payload.queryCacheKey, mwApi);
    }
    if (queryThunk.pending.match(action) || queryThunk.rejected.match(action) && action.meta.condition) {
      schedulePollingUpdate(action.meta.arg.queryCacheKey, mwApi);
    }
    if (queryThunk.fulfilled.match(action) || queryThunk.rejected.match(action) && !action.meta.condition) {
      startNextPoll(action.meta.arg, mwApi);
    }
    if (api.util.resetApiState.match(action)) {
      clearPolls();
      if (pollingUpdateTimer) {
        clearTimeout(pollingUpdateTimer);
        pollingUpdateTimer = null;
      }
      pendingPollingUpdates.clear();
    }
  };
  function schedulePollingUpdate(queryCacheKey, api2) {
    pendingPollingUpdates.add(queryCacheKey);
    if (!pollingUpdateTimer) {
      pollingUpdateTimer = setTimeout(() => {
        for (const key of pendingPollingUpdates) {
          updatePollingInterval({
            queryCacheKey: key
          }, api2);
        }
        pendingPollingUpdates.clear();
        pollingUpdateTimer = null;
      }, 0);
    }
  }
  function getCacheEntrySubscriptions(queryCacheKey, api2) {
    const state = api2.getState()[reducerPath];
    const querySubState = state.queries[queryCacheKey];
    const subscriptions = currentSubscriptions.get(queryCacheKey);
    if (!querySubState || querySubState.status === "uninitialized" /* uninitialized */) return;
    return subscriptions;
  }
  function startNextPoll({
    queryCacheKey
  }, api2) {
    const state = api2.getState()[reducerPath];
    const querySubState = state.queries[queryCacheKey];
    const subscriptions = currentSubscriptions.get(queryCacheKey);
    if (!querySubState || querySubState.status === "uninitialized" /* uninitialized */) return;
    const {
      lowestPollingInterval,
      skipPollingIfUnfocused
    } = findLowestPollingInterval(subscriptions);
    if (!Number.isFinite(lowestPollingInterval)) return;
    const currentPoll = currentPolls.get(queryCacheKey);
    if (currentPoll?.timeout) {
      clearTimeout(currentPoll.timeout);
      currentPoll.timeout = void 0;
    }
    const nextPollTimestamp = Date.now() + lowestPollingInterval;
    currentPolls.set(queryCacheKey, {
      nextPollTimestamp,
      pollingInterval: lowestPollingInterval,
      timeout: setTimeout(() => {
        if (state.config.focused || !skipPollingIfUnfocused) {
          api2.dispatch(refetchQuery(querySubState));
        }
        startNextPoll({
          queryCacheKey
        }, api2);
      }, lowestPollingInterval)
    });
  }
  function updatePollingInterval({
    queryCacheKey
  }, api2) {
    const state = api2.getState()[reducerPath];
    const querySubState = state.queries[queryCacheKey];
    const subscriptions = currentSubscriptions.get(queryCacheKey);
    if (!querySubState || querySubState.status === "uninitialized" /* uninitialized */) {
      return;
    }
    const {
      lowestPollingInterval
    } = findLowestPollingInterval(subscriptions);
    if (false) {
      const updateCounters = currentPolls.pollUpdateCounters ??= {};
      updateCounters[queryCacheKey] ??= 0;
      updateCounters[queryCacheKey]++;
    }
    if (!Number.isFinite(lowestPollingInterval)) {
      cleanupPollForKey(queryCacheKey);
      return;
    }
    const currentPoll = currentPolls.get(queryCacheKey);
    const nextPollTimestamp = Date.now() + lowestPollingInterval;
    if (!currentPoll || nextPollTimestamp < currentPoll.nextPollTimestamp) {
      startNextPoll({
        queryCacheKey
      }, api2);
    }
  }
  function cleanupPollForKey(key) {
    const existingPoll = currentPolls.get(key);
    if (existingPoll?.timeout) {
      clearTimeout(existingPoll.timeout);
    }
    currentPolls.delete(key);
  }
  function clearPolls() {
    for (const key of currentPolls.keys()) {
      cleanupPollForKey(key);
    }
  }
  function findLowestPollingInterval(subscribers = /* @__PURE__ */ new Map()) {
    let skipPollingIfUnfocused = false;
    let lowestPollingInterval = Number.POSITIVE_INFINITY;
    for (const entry of subscribers.values()) {
      if (!!entry.pollingInterval) {
        lowestPollingInterval = Math.min(entry.pollingInterval, lowestPollingInterval);
        skipPollingIfUnfocused = entry.skipPollingIfUnfocused || skipPollingIfUnfocused;
      }
    }
    return {
      lowestPollingInterval,
      skipPollingIfUnfocused
    };
  }
  return handler;
};

// src/query/core/buildMiddleware/queryLifecycle.ts
var buildQueryLifecycleHandler = ({
  api,
  context,
  queryThunk,
  mutationThunk
}) => {
  const isPendingThunk = (0, import_toolkit.isPending)(queryThunk, mutationThunk);
  const isRejectedThunk = (0, import_toolkit.isRejected)(queryThunk, mutationThunk);
  const isFullfilledThunk = (0, import_toolkit.isFulfilled)(queryThunk, mutationThunk);
  const lifecycleMap = {};
  const handler = (action, mwApi) => {
    if (isPendingThunk(action)) {
      const {
        requestId,
        arg: {
          endpointName,
          originalArgs
        }
      } = action.meta;
      const endpointDefinition = context.endpointDefinitions[endpointName];
      const onQueryStarted = endpointDefinition?.onQueryStarted;
      if (onQueryStarted) {
        const lifecycle = {};
        const queryFulfilled = new Promise((resolve, reject) => {
          lifecycle.resolve = resolve;
          lifecycle.reject = reject;
        });
        queryFulfilled.catch(() => {
        });
        lifecycleMap[requestId] = lifecycle;
        const selector = api.endpoints[endpointName].select(isAnyQueryDefinition(endpointDefinition) ? originalArgs : requestId);
        const extra = mwApi.dispatch((_, __, extra2) => extra2);
        const lifecycleApi = {
          ...mwApi,
          getCacheEntry: () => selector(mwApi.getState()),
          requestId,
          extra,
          updateCachedData: isAnyQueryDefinition(endpointDefinition) ? (updateRecipe) => mwApi.dispatch(api.util.updateQueryData(endpointName, originalArgs, updateRecipe)) : void 0,
          queryFulfilled
        };
        onQueryStarted(originalArgs, lifecycleApi);
      }
    } else if (isFullfilledThunk(action)) {
      const {
        requestId,
        baseQueryMeta
      } = action.meta;
      lifecycleMap[requestId]?.resolve({
        data: action.payload,
        meta: baseQueryMeta
      });
      delete lifecycleMap[requestId];
    } else if (isRejectedThunk(action)) {
      const {
        requestId,
        rejectedWithValue,
        baseQueryMeta
      } = action.meta;
      lifecycleMap[requestId]?.reject({
        error: action.payload ?? action.error,
        isUnhandledError: !rejectedWithValue,
        meta: baseQueryMeta
      });
      delete lifecycleMap[requestId];
    }
  };
  return handler;
};

// src/query/core/buildMiddleware/windowEventHandling.ts
var buildWindowEventHandler = ({
  reducerPath,
  context,
  api,
  refetchQuery,
  internalState
}) => {
  const {
    removeQueryResult
  } = api.internalActions;
  const handler = (action, mwApi) => {
    if (onFocus.match(action)) {
      refetchValidQueries(mwApi, "refetchOnFocus");
    }
    if (onOnline.match(action)) {
      refetchValidQueries(mwApi, "refetchOnReconnect");
    }
  };
  function refetchValidQueries(api2, type) {
    const state = api2.getState()[reducerPath];
    const queries = state.queries;
    const subscriptions = internalState.currentSubscriptions;
    context.batch(() => {
      for (const queryCacheKey of subscriptions.keys()) {
        const querySubState = queries[queryCacheKey];
        const subscriptionSubState = subscriptions.get(queryCacheKey);
        if (!subscriptionSubState || !querySubState) continue;
        const values = [...subscriptionSubState.values()];
        const shouldRefetch = values.some((sub) => sub[type] === true) || values.every((sub) => sub[type] === void 0) && state.config[type];
        if (shouldRefetch) {
          if (subscriptionSubState.size === 0) {
            api2.dispatch(removeQueryResult({
              queryCacheKey
            }));
          } else if (querySubState.status !== "uninitialized" /* uninitialized */) {
            api2.dispatch(refetchQuery(querySubState));
          }
        }
      }
    });
  }
  return handler;
};

// src/query/core/buildMiddleware/index.ts
function buildMiddleware(input) {
  const {
    reducerPath,
    queryThunk,
    api,
    context,
    internalState
  } = input;
  const {
    apiUid
  } = context;
  const actions = {
    invalidateTags: (0, import_toolkit.createAction)(`${reducerPath}/invalidateTags`)
  };
  const isThisApiSliceAction = (action) => action.type.startsWith(`${reducerPath}/`);
  const handlerBuilders = [buildDevCheckHandler, buildCacheCollectionHandler, buildInvalidationByTagsHandler, buildPollingHandler, buildCacheLifecycleHandler, buildQueryLifecycleHandler];
  const middleware = (mwApi) => {
    let initialized2 = false;
    const builderArgs = {
      ...input,
      internalState,
      refetchQuery,
      isThisApiSliceAction,
      mwApi
    };
    const handlers = handlerBuilders.map((build) => build(builderArgs));
    const batchedActionsHandler = buildBatchedActionsHandler(builderArgs);
    const windowEventsHandler = buildWindowEventHandler(builderArgs);
    return (next) => {
      return (action) => {
        if (!(0, import_toolkit.isAction)(action)) {
          return next(action);
        }
        if (!initialized2) {
          initialized2 = true;
          mwApi.dispatch(api.internalActions.middlewareRegistered(apiUid));
        }
        const mwApiWithNext = {
          ...mwApi,
          next
        };
        const stateBefore = mwApi.getState();
        const [actionShouldContinue, internalProbeResult] = batchedActionsHandler(action, mwApiWithNext, stateBefore);
        let res;
        if (actionShouldContinue) {
          res = next(action);
        } else {
          res = internalProbeResult;
        }
        if (!!mwApi.getState()[reducerPath]) {
          windowEventsHandler(action, mwApiWithNext, stateBefore);
          if (isThisApiSliceAction(action) || context.hasRehydrationInfo(action)) {
            for (const handler of handlers) {
              handler(action, mwApiWithNext, stateBefore);
            }
          }
        }
        return res;
      };
    };
  };
  return {
    middleware,
    actions
  };
  function refetchQuery(querySubState) {
    return input.api.endpoints[querySubState.endpointName].initiate(querySubState.originalArgs, {
      subscribe: false,
      forceRefetch: true
    });
  }
}

// src/query/core/module.ts
var coreModuleName = /* @__PURE__ */ Symbol();
var coreModule = ({
  createSelector: createSelector2 = import_toolkit.createSelector
} = {}) => ({
  name: coreModuleName,
  init(api, {
    baseQuery,
    tagTypes,
    reducerPath,
    serializeQueryArgs,
    keepUnusedDataFor,
    refetchOnMountOrArgChange,
    refetchOnFocus,
    refetchOnReconnect,
    invalidationBehavior,
    onSchemaFailure,
    catchSchemaFailure,
    skipSchemaValidation
  }, context) {
    (0, import_immer5.enablePatches)();
    assertCast(serializeQueryArgs);
    const assertTagType = (tag) => {
      if (typeof process !== "undefined" && true) {
        if (!tagTypes.includes(tag.type)) {
          console.error(`Tag type '${tag.type}' was used, but not specified in \`tagTypes\`!`);
        }
      }
      return tag;
    };
    Object.assign(api, {
      reducerPath,
      endpoints: {},
      internalActions: {
        onOnline,
        onOffline,
        onFocus,
        onFocusLost
      },
      util: {}
    });
    const selectors = buildSelectors({
      serializeQueryArgs,
      reducerPath,
      createSelector: createSelector2
    });
    const {
      selectInvalidatedBy,
      selectCachedArgsForQuery,
      buildQuerySelector,
      buildInfiniteQuerySelector,
      buildMutationSelector
    } = selectors;
    safeAssign(api.util, {
      selectInvalidatedBy,
      selectCachedArgsForQuery
    });
    const {
      queryThunk,
      infiniteQueryThunk,
      mutationThunk,
      patchQueryData,
      updateQueryData,
      upsertQueryData,
      prefetch,
      buildMatchThunkActions
    } = buildThunks({
      baseQuery,
      reducerPath,
      context,
      api,
      serializeQueryArgs,
      assertTagType,
      selectors,
      onSchemaFailure,
      catchSchemaFailure,
      skipSchemaValidation
    });
    const {
      reducer,
      actions: sliceActions
    } = buildSlice({
      context,
      queryThunk,
      infiniteQueryThunk,
      mutationThunk,
      serializeQueryArgs,
      reducerPath,
      assertTagType,
      config: {
        refetchOnFocus,
        refetchOnReconnect,
        refetchOnMountOrArgChange,
        keepUnusedDataFor,
        reducerPath,
        invalidationBehavior
      }
    });
    safeAssign(api.util, {
      patchQueryData,
      updateQueryData,
      upsertQueryData,
      prefetch,
      resetApiState: sliceActions.resetApiState,
      upsertQueryEntries: sliceActions.cacheEntriesUpserted
    });
    safeAssign(api.internalActions, sliceActions);
    const internalState = {
      currentSubscriptions: /* @__PURE__ */ new Map(),
      currentPolls: /* @__PURE__ */ new Map(),
      runningQueries: /* @__PURE__ */ new Map(),
      runningMutations: /* @__PURE__ */ new Map()
    };
    const {
      buildInitiateQuery,
      buildInitiateInfiniteQuery,
      buildInitiateMutation,
      getRunningMutationThunk,
      getRunningMutationsThunk,
      getRunningQueriesThunk,
      getRunningQueryThunk
    } = buildInitiate({
      queryThunk,
      mutationThunk,
      infiniteQueryThunk,
      api,
      serializeQueryArgs,
      context,
      internalState
    });
    safeAssign(api.util, {
      getRunningMutationThunk,
      getRunningMutationsThunk,
      getRunningQueryThunk,
      getRunningQueriesThunk
    });
    const {
      middleware,
      actions: middlewareActions
    } = buildMiddleware({
      reducerPath,
      context,
      queryThunk,
      mutationThunk,
      infiniteQueryThunk,
      api,
      assertTagType,
      selectors,
      getRunningQueryThunk,
      internalState
    });
    safeAssign(api.util, middlewareActions);
    safeAssign(api, {
      reducer,
      middleware
    });
    return {
      name: coreModuleName,
      injectEndpoint(endpointName, definition) {
        const anyApi = api;
        const endpoint = anyApi.endpoints[endpointName] ??= {};
        if (isQueryDefinition(definition)) {
          safeAssign(endpoint, {
            name: endpointName,
            select: buildQuerySelector(endpointName, definition),
            initiate: buildInitiateQuery(endpointName, definition)
          }, buildMatchThunkActions(queryThunk, endpointName));
        }
        if (isMutationDefinition(definition)) {
          safeAssign(endpoint, {
            name: endpointName,
            select: buildMutationSelector(),
            initiate: buildInitiateMutation(endpointName)
          }, buildMatchThunkActions(mutationThunk, endpointName));
        }
        if (isInfiniteQueryDefinition(definition)) {
          safeAssign(endpoint, {
            name: endpointName,
            select: buildInfiniteQuerySelector(endpointName, definition),
            initiate: buildInitiateInfiniteQuery(endpointName, definition)
          }, buildMatchThunkActions(queryThunk, endpointName));
        }
      }
    };
  }
});

// src/query/core/index.ts
var createApi = /* @__PURE__ */ buildCreateApi(coreModule());
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  NamedSchemaError,
  QueryStatus,
  _NEVER,
  buildCreateApi,
  copyWithStructuralSharing,
  coreModule,
  coreModuleName,
  createApi,
  defaultSerializeQueryArgs,
  fakeBaseQuery,
  fetchBaseQuery,
  retry,
  setupListeners,
  skipToken
});
//# sourceMappingURL=rtk-query.development.cjs.map