package com.example.demosyncmessage;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventSourceResDto{
    private int id;
    private String type;
    private String data;


}
