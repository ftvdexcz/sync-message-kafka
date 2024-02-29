package com.example.demosyncmessage;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MessageDto {
    private String key;
    private String value;
    private long timestamp;
    private int partition;
    private long offset;
    private String data;
}
