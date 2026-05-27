import { useCallback, useEffect, useRef, useState } from "react";
import { sseMultiplexer } from "../utils/sseMultiplexer";

export const SSE_STATUS = {
  IDLE: "idle",
  CONNECTING: "connecting",
  CONNECTED: "connected",
  RECONNECTING: "reconnecting",
};

/**
 * Manages an SSE (Server-Sent Events) connection by delegating to a
 * thread-safe, cross-tab multiplexer. This prevents browser connection pool
 * exhaustion (exceeding the HTTP/1.1 6-connection domain limit) by sharing a
 * single physical EventSource connection across all open tabs.
 *
 * @param {string} path - Endpoint path, e.g. "/stream/leaderboard" or "/stream/analytics"
 * @param {object} [options]
 * @param {function} [options.onMessage] - Called with (parsedData, eventType) on each event
 * @param {boolean} [options.enabled=true]  - Set false to disable the connection
 */
export default function useRealTimeConnection(path, { onMessage, enabled = true } = {}) {
  const [status, setStatus] = useState(SSE_STATUS.IDLE);
  
  // Stable reference to callback ensures the connection does not restart on prop changes
  const onMessageRef = useRef(onMessage);
  useEffect(() => { onMessageRef.current = onMessage; }, [onMessage]);

  const teardown = useCallback(() => {
    clearTimeout(retryTimer.current);
    if (sourceRef.current) {
      sourceRef.current.close();
      sourceRef.current = null;
    }
  }, []);

  const connect = useCallback(() => {
    teardown();

    let source;
    try {
      // Safari throws a synchronous SecurityError when EventSource is blocked
      // (e.g. cross-origin with credentials when backend is unreachable).
      // Catch it here so it doesn't crash the app — we'll retry with backoff.
      source = new EventSource(`${SSE_BASE_URL}${path}`, { withCredentials: true });
    } catch (err) {
      const delay = computeBackoff(attemptRef.current);
      attemptRef.current += 1;
      setStatus(SSE_STATUS.RECONNECTING);
      retryTimer.current = setTimeout(connect, delay);
      return;
    }

    sourceRef.current = source;
    setStatus(attemptRef.current === 0 ? SSE_STATUS.CONNECTING : SSE_STATUS.RECONNECTING);

    source.onopen = () => {
      attemptRef.current = 0;
      setStatus(SSE_STATUS.CONNECTED);
    };

    source.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        onMessageRef.current?.(data, evt.type);
      } catch {
        // Forward raw string if JSON parsing fails
        onMessageRef.current?.(evt.data, evt.type);
      }
    };

    source.onerror = () => {
      // EventSource fires onerror on any failure (network drop, 4xx, 5xx).
      // We close it manually so our backoff timer controls reconnection timing.
      source.close();
      sourceRef.current = null;

      const delay = computeBackoff(attemptRef.current);
      attemptRef.current += 1;
      setStatus(SSE_STATUS.RECONNECTING);
      retryTimer.current = setTimeout(connect, delay);
    };
  }, [path, teardown]);

  useEffect(() => {
    if (!enabled) {
      teardown();
      setStatus(SSE_STATUS.IDLE);
      return;
    }
    attemptRef.current = 0;
    connect();
    return teardown;
  }, [path, enabled, connect, teardown]);

  const reconnect = useCallback(() => {
    sseMultiplexer.reconnect(path);
  }, [path]);

  return { status, reconnect };
}