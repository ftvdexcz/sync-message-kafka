package com.example.demosyncmessage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/messages")
public class MessageController {
    @Autowired
    MessageService messageService;

    @GetMapping("/sync")
    public List<MessageDto> syncMessages(){
        return messageService.syncMessages();
    }

    @GetMapping(value = "/sync-sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<MessageDto> syncMessagesSSE(){
        return messageService.syncMessagesFlux();
    }

    @GetMapping("/sync-sse-emitter")
    public ResponseBodyEmitter syncMessagesSseEmitter(){
        return messageService.syncMessagesSseEmitter();
    }
}
