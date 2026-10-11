package com.gridwatch.ingestion.poll;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Maps the raw CSMS poll API response shape.
 * Handles both normal and malformed pages — fields may be null.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CsmsPollerResponse {

    @JsonProperty("site_id")
    private String siteId;

    @JsonProperty("server_time")
    private String serverTime;

    /** Opaque cursor for pagination. Absence means this is the final page. */
    @JsonProperty("next_cursor")
    private String nextCursor;

    /** Raw event list — individual events may be malformed */
    @JsonProperty("events")
    private List<Map<String, Object>> events;
}
