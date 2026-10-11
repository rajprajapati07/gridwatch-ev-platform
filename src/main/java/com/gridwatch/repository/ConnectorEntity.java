package com.gridwatch.repository;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;

@Data
@Entity
@Table(name = "connectors")
public class ConnectorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "charger_id", nullable = false)
    private String chargerId;

    @Column(name = "connector_id", nullable = false)
    private String connectorId;

    @Column(name = "operator_id", nullable = false)
    private String operatorId;

    @Column(name = "status")
    private String status;

    @Column(name = "status_ts")
    private OffsetDateTime statusTs;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;
}
