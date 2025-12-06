package com.example.cdc.service;

import com.example.cdc.entity.CustomerEntity;
import com.example.cdc.entity.ProcessedEvent;
import com.example.cdc.repo.CustomerRepository;
import com.example.cdc.repo.ProcessedEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class CustomerService {

    private final CustomerRepository customerRepository;
    private final ProcessedEventRepository processedEventRepository;

    public CustomerService(CustomerRepository customerRepository, ProcessedEventRepository processedEventRepository) {
        this.customerRepository = customerRepository;
        this.processedEventRepository = processedEventRepository;
    }

    public boolean isProcessed(String eventId) {
        return processedEventRepository.existsById(eventId);
    }

    @Transactional
    public void handleEvent(JsonNode payload, String op) {
        if ("c".equals(op) || "r".equals(op)) {
            JsonNode after = payload.get("after");
            if (after != null && !after.isNull()) {
                CustomerEntity e = toEntity(after);
                customerRepository.save(e);
            }
        } else if ("u".equals(op)) {
            JsonNode after = payload.get("after");
            if (after != null && !after.isNull()) {
                CustomerEntity e = toEntity(after);
                Optional<CustomerEntity> existing = customerRepository.findById(e.getId());
                if (existing.isPresent()) {
                    CustomerEntity ex = existing.get();
                    ex.setName(e.getName());
                    ex.setEmail(e.getEmail());
                    customerRepository.save(ex);
                } else {
                    // if not exists, create
                    customerRepository.save(e);
                }
            }
        } else if ("d".equals(op)) {
            JsonNode before = payload.get("before");
            if (before != null && !before.isNull()) {
                Long id = before.get("id").asLong();
                customerRepository.deleteById(id);
            }
        } else {
            // unknown op - ignore or log
            System.out.println("Unknown op: " + op);
        }
    }

    @Transactional
    public void markProcessed(String eventId) {
        processedEventRepository.save(new ProcessedEvent(eventId));
    }

    private CustomerEntity toEntity(JsonNode node) {
        CustomerEntity c = new CustomerEntity();
        c.setId(node.get("id").asLong());
        c.setName(node.has("name") && !node.get("name").isNull() ? node.get("name").asText() : null);
        c.setEmail(node.has("email") && !node.get("email").isNull() ? node.get("email").asText() : null);
        return c;
    }
}
