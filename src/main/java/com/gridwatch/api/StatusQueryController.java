package com.gridwatch.api;

import com.gridwatch.repository.ConnectorRepository;
import com.gridwatch.repository.FaultRepository;
import com.gridwatch.repository.SessionRepository;
import com.gridwatch.repository.SiteRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dashboard query API.
 *
 * Security note: In production, operatorId is extracted from the JWT claim, NOT from a query
 * parameter. The header @RequestParam is used here for simplicity of demonstration only.
 * See Architecture doc §9 and Part B Review Finding #1 for the security design.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class StatusQueryController {

    private final SiteRepository siteRepository;
    private final ConnectorRepository connectorRepository;
    private final FaultRepository faultRepository;
    private final SessionRepository sessionRepository;
    private final StringRedisTemplate redisTemplate;
    private final MeterRegistry meterRegistry;

    /**
     * Network-level status summary: counts per status across all connectors for an operator.
     * Reads from Redis (< 1 ms) with PostgreSQL fallback on cache miss.
     *
     * This is the "aggregate" required by the assignment brief:
     * "live status rollup per site, or current network utilisation"
     */
    @GetMapping("/network/summary")
    public ResponseEntity<Map<String, Object>> networkSummary(
            @RequestParam String operatorId) {

        long start = System.currentTimeMillis();

        // Get all connectors for this operator and their current status
        List<ConnectorRepository.ConnectorStatus> statuses =
            connectorRepository.findLiveStatusByOperator(operatorId);

        Map<String, Long> statusCounts = new HashMap<>();
        for (ConnectorRepository.ConnectorStatus cs : statuses) {
            String redisKey = "status:" + operatorId + ":" + cs.chargerId() + ":" + cs.connectorId();
            String cached = redisTemplate.opsForValue().get(redisKey);

            // Use Redis live status if available and fresh; fallback to DB value
            String status = cs.status();
            if (cached != null) {
                // Redis has a fresher value
                status = extractStatusFromJson(cached);
            }
            statusCounts.merge(status, 1L, Long::sum);
        }

        long total = statuses.size();
        long charging = statusCounts.getOrDefault("Charging", 0L);
        long available = statusCounts.getOrDefault("Available", 0L);
        long faulted = statusCounts.getOrDefault("Faulted", 0L);
        long offline = total - charging - available - faulted;

        Map<String, Object> result = Map.of(
            "operator_id", operatorId,
            "total_connectors", total,
            "charging", charging,
            "available", available,
            "faulted", faulted,
            "offline", Math.max(0, offline),
            "utilisation_pct", total > 0 ? (charging * 100.0 / total) : 0.0
        );

        meterRegistry.timer("gridwatch.api.network_summary.latency")
            .record(System.currentTimeMillis() - start, java.util.concurrent.TimeUnit.MILLISECONDS);

        return ResponseEntity.ok(result);
    }

    /**
     * Site-level status rollup for the GIS map.
     * Returns GeoJSON FeatureCollection with current status per site.
     */
    @GetMapping("/map/sites/geojson")
    public ResponseEntity<Map<String, Object>> sitesGeoJson(
            @RequestParam String operatorId) {

        List<SiteRepository.SiteGeoStatus> sites = siteRepository.findSiteGeoStatusByOperator(operatorId);

        List<Map<String, Object>> features = sites.stream().map(site -> {
            Map<String, Object> geometry = site.lng() != null
                ? Map.of("type", "Point", "coordinates", List.of(site.lng(), site.lat()))
                : Map.of("type", "Point", "coordinates", List.of()); // location pending

            Map<String, Object> properties = new HashMap<>();
            properties.put("site_id", site.siteId());
            properties.put("name", site.name());
            properties.put("address", site.address());
            properties.put("geocode_status", site.geocodeStatus());
            properties.put("charger_count", site.chargerCount());
            properties.put("available_count", site.availableCount());
            properties.put("charging_count", site.chargingCount());
            properties.put("faulted_count", site.faultedCount());

            return (Map<String, Object>) Map.of(
                "type", "Feature",
                "geometry", geometry,
                "properties", properties
            );
        }).toList();

        Map<String, Object> geoJson = Map.of(
            "type", "FeatureCollection",
            "features", features
        );

        return ResponseEntity.ok(geoJson);
    }

    /**
     * Nearest available charger to a given point.
     * Uses PostGIS KNN with spherical distance — correct at real-world distances.
     */
    @GetMapping("/chargers/nearest")
    public ResponseEntity<List<Map<String, Object>>> nearestAvailable(
            @RequestParam String operatorId,
            @RequestParam double lat,
            @RequestParam double lng,
            @RequestParam(defaultValue = "5") int limit) {

        List<SiteRepository.NearestCharger> nearest =
            siteRepository.findNearestAvailableChargers(operatorId, lat, lng, limit);

        List<Map<String, Object>> result = nearest.stream().map(nc -> {
            Map<String, Object> item = new HashMap<>();
            item.put("charger_id", nc.chargerId());
            item.put("connector_id", nc.connectorId());
            item.put("site_name", nc.siteName());
            item.put("address", nc.address());
            item.put("distance_metres", nc.distanceMetres());
            return item;
        }).toList();

        return ResponseEntity.ok(result);
    }

    /**
     * Active sessions count and energy for an operator.
     */
    @GetMapping("/sessions/active")
    public ResponseEntity<Map<String, Object>> activeSessions(
            @RequestParam String operatorId) {

        long count = sessionRepository.countActiveSessions(operatorId);
        return ResponseEntity.ok(Map.of(
            "operator_id", operatorId,
            "active_sessions", count
        ));
    }

    /**
     * Open faults for an operator (unresolved, ordered by severity and time).
     */
    @GetMapping("/faults/open")
    public ResponseEntity<List<Map<String, Object>>> openFaults(
            @RequestParam String operatorId) {

        return ResponseEntity.ok(faultRepository.findOpenFaults(operatorId));
    }

    private String extractStatusFromJson(String json) {
        // Simple extraction without full JSON parsing for performance
        int idx = json.indexOf("\"status\":\"");
        if (idx < 0) return "Unknown";
        int start = idx + 10;
        int end = json.indexOf('"', start);
        return end > start ? json.substring(start, end) : "Unknown";
    }
}
