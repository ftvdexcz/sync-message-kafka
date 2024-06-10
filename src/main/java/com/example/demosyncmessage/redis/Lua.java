package com.example.demosyncmessage.redis;

import com.example.demosyncmessage.MessageDto;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.LuaTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class Lua {
    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    MessageDto a = MessageDto.builder()
            .key("a")
            .value("a")
            .timestamp(System.currentTimeMillis())
            .partition(0)
            .offset(0)
            .build();
    MessageDto b = MessageDto.builder()
            .key("b")
            .value("b")
            .timestamp(System.currentTimeMillis())
            .partition(1)
            .offset(1)
            .build();
    List<MessageDto> argv1 = new ArrayList<>(List.of(a, b));
    List<MessageDto> argv2 = new ArrayList<>(argv1);

    @PostConstruct
    public void run(){
        Resource scriptSource = new ClassPathResource("scripts/push.lua");
        RedisScript<Boolean> script = RedisScript.of(scriptSource, Boolean.class);
        Gson gson = new Gson();

        argv2.remove(b);

        redisTemplate.opsForSet().add("test2", argv1.toArray());
        redisTemplate.execute(new SessionCallback<List<Object>>() {
            @Override
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                try{
                    operations.execute(script, List.of("test2", "test"), List.of(gson.toJson(argv1), gson.toJson(argv2)).toArray());
                    List<Object> exec = operations.exec();
                    return exec;
                }catch (Exception ex){
                    log.error("Execution error: {}", ex.getMessage());
                    operations.discard();
                }
                return null;
            }
        });

    }
}
