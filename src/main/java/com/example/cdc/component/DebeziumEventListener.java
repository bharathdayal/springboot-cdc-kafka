package com.example.cdc.component;

import com.example.cdc.service.CustomerService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class DebeziumEventListener {

    private final ObjectMapper mapper = new ObjectMapper();
    private final CustomerService customerService;

    public DebeziumEventListener(CustomerService customerService) {
        this.customerService = customerService;
    }

    @KafkaListener(topics = "${debezium.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String value = record.value();
            if (value == null || value.isBlank()) {
                ack.acknowledge();
                return;
            }

            JsonNode root = mapper.readTree(value);

            // support both forms:
            // 1) { "schema": {...}, "payload": { ... } }
            // 2) { "before":..., "after":..., "op":..., "source":... }
            JsonNode payload = root.has("payload") ? root.get("payload") : root;

            if (payload == null || payload.isNull()) {
                ack.acknowledge();
                return;
            }

            String op = payload.has("op") && !payload.get("op").isNull()
                    ? payload.get("op").asText()
                    : null;

            // Build event id using binlog source info if available (file:pos:ts_ms) — stable dedup key
            String eventId = buildEventId(payload);

            if (eventId != null && customerService.isProcessed(eventId)) {
                ack.acknowledge();
                return;
            }

            // Delegate to service: service should handle c/u/d/r based on 'op' and payload nodes
            customerService.handleEvent(payload, op);

            // mark processed using the chosen eventId (or a fallback)
            if (eventId == null) {
                // fallback: hash of payload + Kafka offset (less ideal)
                eventId = "fallback-" + record.topic() + "-" + record.partition() + "-" + record.offset();
            }
            customerService.markProcessed(eventId);

            ack.acknowledge();
        } catch (Exception ex) {
            // rethrow to trigger DefaultErrorHandler -> retries -> DLT
            throw new RuntimeException("Failed to process CDC record", ex);
        }
    }

    private String buildEventId(JsonNode payload) {
        JsonNode source = payload.get("source");
        if (source != null && !source.isNull()) {
            String file = source.has("file") && !source.get("file").isNull() ? source.get("file").asText("") : "";
            String pos = source.has("pos") && !source.get("pos").isNull() ? source.get("pos").asText("") : "";
            String ts = source.has("ts_ms") && !source.get("ts_ms").isNull() ? source.get("ts_ms").asText("") : "";
            if (!file.isEmpty() && !pos.isEmpty()) {
                return file + ":" + pos + ":" + ts;
            }
            // fallback if no file/pos but has gtid / server_id etc:
            if (source.has("gtid") && !source.get("gtid").isNull()) {
                return source.get("gtid").asText();
            }
        }
        return null;
    }
}
