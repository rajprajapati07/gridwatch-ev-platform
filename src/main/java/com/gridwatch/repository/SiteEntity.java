package com.gridwatch.repository;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "sites")
public class SiteEntity {

    @Id
    @Column(name = "site_id")
    private String siteId;

    @Column(name = "operator_id", nullable = false)
    private String operatorId;

    @Column(name = "name")
    private String name;

    @Column(name = "address")
    private String address;

    @Column(name = "geocode_status")
    private String geocodeStatus;

    // location stored as PostGIS GEOMETRY — not mapped via JPA (use native queries)
}
