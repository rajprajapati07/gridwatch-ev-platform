package com.gridwatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
public interface SitePollStateRepository extends JpaRepository<SitePollStateEntity, String> {

    /**
     * Returns site IDs whose next_poll_at is now or in the past — i.e., due for polling.
     * Called every 5 seconds by CsmsPollScheduler.
     */
    @Query(value = """
        SELECT site_id
          FROM site_poll_state
         WHERE next_poll_at <= :now
         ORDER BY next_poll_at ASC
         LIMIT 500
        """, nativeQuery = true)
    List<String> findSitesDueForPoll(@Param("now") OffsetDateTime now);
}
