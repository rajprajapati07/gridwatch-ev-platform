package com.gridwatch.ingestion.poll;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * HTTP client for the CSMS poll API.
 *
 * Handles:
 * - Pagination (next_cursor)
 * - 429 rate limiting (backs off for retry_after_seconds)
 * - 500 server errors (exponential retry, up to maxRetries)
 * - Malformed JSON (per-event; handled in CsmsEventNormaliser)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CsmsPollClient {

    private final RestTemplate restTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${gridwatch.csms.base-url}")
    private String baseUrl;

    @Value("${gridwatch.csms.max-retries:3}")
    private int maxRetries;

    @Value("${gridwatch.csms.retry-backoff-seconds:5}")
    private int retryBackoffSeconds;

    /**
     * Fetch one page of events for a site.
     *
     * @param siteId    the site to poll
     * @param since     high-water mark timestamp (ISO 8601 with offset)
     * @param cursor    pagination cursor (null for first page)
     * @return PollResult — either the page, a rate-limit signal with retry_after, or a terminal error
     */
    public PollResult fetchPage(String siteId, OffsetDateTime since, String cursor) {
        String url = buildUrl(siteId, since, cursor);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Polling CSMS: site={} cursor={} attempt={}", siteId, cursor, attempt);

                ResponseEntity<CsmsPollerResponse> response =
                    restTemplate.getForEntity(url, CsmsPollerResponse.class);

                meterRegistry.counter("gridwatch.csms.poll.success", "site_id", siteId).increment();
                return PollResult.success(response.getBody());

            } catch (HttpClientErrorException e) {
                if (e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                    // 429 — parse retry_after from response body or header
                    int retryAfter = parseRetryAfter(e);
                    log.warn("CSMS rate limited for site={}, retry after {}s", siteId, retryAfter);
                    meterRegistry.counter("gridwatch.csms.poll.rate_limited", "site_id", siteId).increment();
                    return PollResult.rateLimited(retryAfter);
                }
                log.error("CSMS poll client error: site={} status={} body={}",
                    siteId, e.getStatusCode(), e.getResponseBodyAsString());
                return PollResult.error("Client error: " + e.getStatusCode());

            } catch (HttpServerErrorException e) {
                // 500 — transient, retry with backoff
                log.warn("CSMS poll server error: site={} status={} attempt={}/{}",
                    siteId, e.getStatusCode(), attempt, maxRetries);
                meterRegistry.counter("gridwatch.csms.poll.server_error", "site_id", siteId).increment();

                if (attempt < maxRetries) {
                    sleepQuietly(retryBackoffSeconds * (long) Math.pow(2, attempt - 1));
                } else {
                    return PollResult.error("Server error after " + maxRetries + " retries");
                }

            } catch (Exception e) {
                log.error("Unexpected error polling CSMS for site={}: {}", siteId, e.getMessage(), e);
                return PollResult.error("Unexpected: " + e.getMessage());
            }
        }

        return PollResult.error("Exhausted retries");
    }

    private String buildUrl(String siteId, OffsetDateTime since, String cursor) {
        StringBuilder sb = new StringBuilder(baseUrl)
            .append("/v1/chargers?site_id=").append(siteId);

        if (since != null) {
            sb.append("&since=").append(since.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
        if (cursor != null && !cursor.isBlank()) {
            sb.append("&cursor=").append(cursor);
        }
        return sb.toString();
    }

    private int parseRetryAfter(HttpClientErrorException e) {
        // Try Retry-After header first
        String header = e.getResponseHeaders() != null
            ? e.getResponseHeaders().getFirst("Retry-After") : null;
        if (header != null) {
            try { return Integer.parseInt(header); } catch (NumberFormatException ignored) {}
        }
        // Default from the brief: 30 s
        return 30;
    }

    private void sleepQuietly(long seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ─── Result type ─────────────────────────────────────────────────────────

    public record PollResult(
        Status status,
        CsmsPollerResponse page,
        int retryAfterSeconds,
        String errorMessage
    ) {
        public enum Status { SUCCESS, RATE_LIMITED, ERROR }

        public static PollResult success(CsmsPollerResponse page) {
            return new PollResult(Status.SUCCESS, page, 0, null);
        }
        public static PollResult rateLimited(int retryAfter) {
            return new PollResult(Status.RATE_LIMITED, null, retryAfter, null);
        }
        public static PollResult error(String message) {
            return new PollResult(Status.ERROR, null, 0, message);
        }

        public boolean isSuccess() { return status == Status.SUCCESS; }
        public boolean isRateLimited() { return status == Status.RATE_LIMITED; }
    }
}
