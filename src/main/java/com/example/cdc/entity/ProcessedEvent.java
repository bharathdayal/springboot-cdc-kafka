package com.example.cdc.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    private String eventId;
    private long processedAt;

    protected ProcessedEvent() {}

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = System.currentTimeMillis();
    }


}
