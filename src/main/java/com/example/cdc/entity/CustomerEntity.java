package com.example.cdc.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "customers")
public class CustomerEntity {
    @Id
    private Long id;
    private String name;
    private String email;
}
