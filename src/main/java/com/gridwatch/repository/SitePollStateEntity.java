package com.gridwatch.repository;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;

@Data
@Entity
@Table(name = "site_poll_state")
public class SitePollStateEntity {

    @Id
    @Column(name = "site_id")
    private String siteId;

    @Column(name = "operator_id", nullable = false)
    private String operatorId;

    @Column(name = "last_polled_at")
    private OffsetDateTime lastPolledAt;

    @Column(name = "last_event_ts")
    private OffsetDateTime lastEventTs;

    @Column(name = "next_poll_at")
    private OffsetDateTime nextPollAt;

    @Column(name = "cursor")
    private String cursor;

    @Column(name = "consecutive_errors")
    private int consecutiveErrors;
}
