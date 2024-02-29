package com.example.demosyncmessage;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventSourceResDto<T> {
    private int id;
    private int count;
    private T data;
}
