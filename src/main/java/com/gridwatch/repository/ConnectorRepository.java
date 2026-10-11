package com.gridwatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
public interface ConnectorRepository extends JpaRepository<ConnectorEntity, Long> {

    /**
     * Compare-and-swap STATUS update: only updates if the incoming event is strictly newer
     * than the current status_ts. This is the core fix for out-of-order event handling.
     *
     * Brief requirement: out-of-order webhooks must NOT overwrite a newer status with a stale one.
     * Solution: WHERE status_ts IS NULL OR status_ts < :eventTs
     *
     * @return rows updated (0 = event was stale, skipped; 1 = updated)
     */
    @Modifying
    @Query(value = """
        UPDATE connectors
           SET status     = :status,
               status_ts  = :eventTs,
               updated_at = NOW()
         WHERE charger_id    = :chargerId
           AND connector_id  = :connectorId
           AND (status_ts IS NULL OR status_ts < :eventTs)
        """, nativeQuery = true)
    int updateStatusIfNewer(
        @Param("chargerId") String chargerId,
        @Param("connectorId") String connectorId,
        @Param("status") String status,
        @Param("eventTs") OffsetDateTime eventTs
    );

    /**
     * Get live status for all connectors belonging to an operator.
     * Used by the dashboard summary endpoint.
     */
    @Query(value = """
        SELECT c.charger_id AS chargerId,
               c.connector_id AS connectorId,
               c.status,
               c.status_ts AS statusTs
          FROM connectors c
          JOIN chargers ch ON ch.charger_id = c.charger_id
         WHERE ch.operator_id = :operatorId
        """, nativeQuery = true)
    List<ConnectorStatus> findLiveStatusByOperator(@Param("operatorId") String operatorId);

    /**
     * Projection for dashboard status counts.
     */
    interface ConnectorStatus {
        String chargerId();
        String connectorId();
        String status();
        OffsetDateTime statusTs();
    }
}
