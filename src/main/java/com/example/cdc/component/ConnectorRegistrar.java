package com.example.cdc.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class ConnectorRegistrar {

    private final RestClient rest;
    private final String connectBaseUrl;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConnectorRegistrar(@Value("${debezium.connect.url}") String baseUrl) {
        this.connectBaseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.rest = RestClient.builder()
                .baseUrl(connectBaseUrl)
                .build();
    }

    @Value("${debezium.registrar.enabled}")
    private boolean enabled;

    @Value("${debezium.registrar.retries}")
    private int retries;

    @Value("${debezium.registrar.retry-delay-ms}")
    private long retryDelayMs;

    @Value("${debezium.connector.name}")
    private String connectorName;

    @Value("${debezium.connector.class:io.debezium.connector.mysql.MySqlConnector}")
    private String connectorClass;

    // Source MySQL (Debezium reads this)
    @Value("${source.mysql.host}")
    private String sourceHost;

    @Value("${source.mysql.port}")
    private String sourcePort;

    @Value("${source.mysql.database}")
    private String sourceDatabase;

    @Value("${source.mysql.user}")
    private String sourceUser;

    @Value("${source.mysql.password}")
    private String sourcePassword;

    @Value("${source.mysql.server-id:184054}")
    private String sourceServerId;

    // Table includes (optional)
    @Value("${debezium.table.include.list:}")
    private String tableInclude;

    // Kafka bootstrap
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrap;

    // Topic prefix overrides default <database.server.name>
    @Value("${topic.prefix:}")
    private String topicPrefix;

    // Schema history topic
    @Value("${debezium.schema.history.topic:}")
    private String historyTopic;

    // Misc Debezium settings
    @Value("${debezium.include.schema.changes:true}")
    private String includeSchemaChanges;

    @Value("${debezium.decimal.handling.mode:double}")
    private String decimalMode;

    @Value("${debezium.tasks.max:1}")
    private String tasksMax;

    @Value("${debezium.snapshot.mode:initial}")
    private String snapshotMode;

    @PostConstruct
    public void registerConnector() throws InterruptedException {
        if (!enabled) {
            System.out.println("[ConnectorRegistrar] Registration disabled.");
            return;
        }

        // Check if Kafka Connect is reachable
        URI uri = URI.create(connectBaseUrl);
        String host = uri.getHost();
        int port = uri.getPort() == -1 ? 8083 : uri.getPort();

        if (!isTcpReachable(host, port, 3000)) {
            throw new IllegalStateException(
                    "[ConnectorRegistrar] Cannot reach Kafka Connect at " + host + ":" + port
            );
        }

        String connectorEndpoint = "/connectors/" + connectorName;
        String registerEndpoint = "/connectors";

        ObjectNode configNode = buildConnectorConfig();
        ObjectNode fullRegistrationNode = objectMapper.createObjectNode();
        fullRegistrationNode.put("name", connectorName);
        fullRegistrationNode.set("config", configNode);

        String configJson = toJson(configNode);
        String registrationJson = toJson(fullRegistrationNode);

        for (int attempt = 1; attempt <= retries; attempt++) {
            try {
                // Check if connector exists
                rest.get()
                        .uri(connectorEndpoint)
                        .retrieve()
                        .toBodilessEntity();

                System.out.println("[ConnectorRegistrar] Connector exists → Updating config");

                rest.put()
                        .uri(connectorEndpoint + "/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(configJson)
                        .retrieve()
                        .toBodilessEntity();

                System.out.println("[ConnectorRegistrar] Connector UPDATED successfully.");
                return;

            } catch (Exception getEx) {
                System.out.printf("[ConnectorRegistrar] Attempt %d: GET failed → try POST: %s%n",
                        attempt, summarize(getEx));

                try {
                    rest.post()
                            .uri(registerEndpoint)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(registrationJson)
                            .retrieve()
                            .toBodilessEntity();

                    System.out.println("[ConnectorRegistrar] Connector REGISTERED successfully.");
                    return;

                } catch (Exception postEx) {
                    System.err.printf("[ConnectorRegistrar] Attempt %d: POST failed: %s%n",
                            attempt, summarize(postEx));
                }
            }

            Thread.sleep(retryDelayMs);
        }

        throw new IllegalStateException(
                "[ConnectorRegistrar] FAILED after " + retries + " attempts."
        );
    }

    private ObjectNode buildConnectorConfig() {
        ObjectNode cfg = objectMapper.createObjectNode();

        cfg.put("connector.class", connectorClass);
        cfg.put("tasks.max", tasksMax);

        // Source DB → Read by Debezium
        cfg.put("database.hostname", sourceHost);
        cfg.put("database.port", sourcePort);
        cfg.put("database.user", sourceUser);
        cfg.put("database.password", sourcePassword);
        cfg.put("database.server.id", sourceServerId);
        cfg.put("database.include.list", sourceDatabase);

        // Topic prefix logic
        String prefix = StringUtils.hasText(topicPrefix) ? topicPrefix : sourceDatabase;
        cfg.put("database.server.name", prefix);
        cfg.put("topic.prefix", prefix);

        // Tables
        if (StringUtils.hasText(tableInclude)) {
            cfg.put("table.include.list", tableInclude);
        }

        // Schema history
        String schemaTopic = StringUtils.hasText(historyTopic)
                ? historyTopic
                : "schema-changes." + sourceDatabase;

        cfg.put("schema.history.internal.kafka.bootstrap.servers", kafkaBootstrap);
        cfg.put("schema.history.internal.kafka.topic", schemaTopic);

        // Backward compatible keys (some Debezium versions still accept them)
        cfg.put("database.history.kafka.bootstrap.servers", kafkaBootstrap);
        cfg.put("database.history.kafka.topic", schemaTopic);

        // Debezium misc
        cfg.put("include.schema.changes", includeSchemaChanges);
        cfg.put("decimal.handling.mode", decimalMode);
        cfg.put("snapshot.mode", snapshotMode);

        return cfg;
    }

    private boolean isTcpReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private String toJson(ObjectNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert config to JSON", e);
        }
    }

    private String summarize(Throwable ex) {
        if (ex == null) return "";
        return ex.getClass().getSimpleName() + ": " + ex.getMessage();
    }
}
