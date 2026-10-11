package com.gridwatch.ingestion.poll;

import com.gridwatch.model.CsmsEvent;
import com.gridwatch.repository.SitePollStateRepository;
import com.gridwatch.repository.SiteRepository;
import com.gridwatch.service.EventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Drives the poll backstop: polls each site's CSMS endpoint at the minimum allowed interval (30s).
 *
 * Design decisions:
 * - Uses a thread pool sized to number_of_sites / poll_interval to keep all sites polling on schedule
 * - Sites polled in parallel (independent per-site state)
 * - Poll state (cursor, high-water mark, next_poll_at) persisted in PostgreSQL for crash recovery
 * - On 429: respects retry_after_seconds; backs off that site only
 * - On 500: retries with backoff inside CsmsPollClient; increments consecutive_errors in state
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsmsPollScheduler {

    private final SiteRepository siteRepository;
    private final SitePollStateRepository pollStateRepository;
    private final CsmsPollClient pollClient;
    private final CsmsEventNormaliser normaliser;
    private final EventPublisher eventPublisher;
    private final MeterRegistry meterRegistry;

    @Value("${gridwatch.csms.poll-interval-seconds:30}")
    private int pollIntervalSeconds;

    // Thread pool for concurrent site polling
    // At 12,000 sites / 30s, we need to process ~400 sites/s.
    // With ~200 ms per poll call (network RTT), 100 threads sustain this.
    private final ThreadPoolExecutor executor = (ThreadPoolExecutor)
        Executors.newFixedThreadPool(100);

    /**
     * Every 5 seconds: find sites whose next_poll_at is now or past, submit to pool.
     */
    @Scheduled(fixedDelay = 5000)
    public void schedulePendingSites() {
        List<String> sitesToPoll = pollStateRepository.findSitesDueForPoll(OffsetDateTime.now());

        if (!sitesToPoll.isEmpty()) {
            log.debug("Scheduling {} sites for poll", sitesToPoll.size());
        }

        for (String siteId : sitesToPoll) {
            executor.submit(() -> {
                try {
                    pollSite(siteId);
                } catch (Exception e) {
                    log.error("Unhandled error polling site={}: {}", siteId, e.getMessage(), e);
                }
            });
        }

        meterRegistry.gauge("gridwatch.poll.executor.queue_size",
            executor.getQueue().size());
    }

    private void pollSite(String siteId) {
        var state = pollStateRepository.findById(siteId).orElseThrow();
        var operator = siteRepository.findOperatorBySiteId(siteId);
        if (operator == null) {
            log.warn("No operator found for site={}", siteId);
            return;
        }

        String cursor = state.getCursor();
        OffsetDateTime highWaterMark = state.getLastEventTs();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        int totalEventsProcessed = 0;
        boolean hasMorePages = true;

        while (hasMorePages) {
            CsmsPollClient.PollResult result = pollClient.fetchPage(siteId, highWaterMark, cursor);

            if (result.isRateLimited()) {
                // Back off this site; schedule retry after rate limit window
                state.setNextPollAt(now.plusSeconds(result.retryAfterSeconds()));
                state.setConsecutiveErrors(state.getConsecutiveErrors() + 1);
                pollStateRepository.save(state);
                log.warn("Site {} rate limited; rescheduled in {}s", siteId, result.retryAfterSeconds());
                return;
            }

            if (!result.isSuccess()) {
                state.setConsecutiveErrors(state.getConsecutiveErrors() + 1);
                state.setNextPollAt(now.plusSeconds(
                    pollIntervalSeconds * Math.min(state.getConsecutiveErrors(), 8)));
                pollStateRepository.save(state);
                return;
            }

            // ─── Process page ───────────────────────────────────────────────
            var page = result.page();
            if (page == null || page.getEvents() == null) {
                break;
            }

            OffsetDateTime latestEventTs = highWaterMark;

            for (Map<String, Object> rawEvent : page.getEvents()) {
                Optional<CsmsEvent> maybeEvent = normaliser.normalise(
                    rawEvent, operator, CsmsEvent.IngestionSource.POLL);

                if (maybeEvent.isPresent()) {
                    CsmsEvent event = maybeEvent.get();
                    eventPublisher.publish(event);
                    totalEventsProcessed++;

                    // Track high-water mark
                    if (latestEventTs == null || event.getEventTs().isAfter(latestEventTs)) {
                        latestEventTs = event.getEventTs();
                    }
                }
            }

            // Update cursor and high-water mark
            cursor = page.getNextCursor();
            highWaterMark = latestEventTs;
            hasMorePages = cursor != null && !cursor.isBlank();
        }

        // ─── Persist updated poll state ──────────────────────────────────────
        state.setLastPolledAt(now);
        state.setLastEventTs(highWaterMark);
        state.setCursor(null); // Reset cursor; next poll starts fresh from since=highWaterMark
        state.setConsecutiveErrors(0);
        state.setNextPollAt(now.plusSeconds(pollIntervalSeconds));
        pollStateRepository.save(state);

        if (totalEventsProcessed > 0) {
            log.info("Polled site={} events={} nextPoll={}", siteId, totalEventsProcessed, state.getNextPollAt());
        }

        meterRegistry.counter("gridwatch.csms.poll.events_processed",
            "site_id", siteId).increment(totalEventsProcessed);
    }
}
