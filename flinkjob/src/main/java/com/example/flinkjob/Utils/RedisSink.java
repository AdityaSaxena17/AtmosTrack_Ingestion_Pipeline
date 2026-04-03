package com.example.flinkjob.Utils;

import org.apache.flink.configuration.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.example.flinkjob.Model.CityAqiResult;

import redis.clients.jedis.Jedis;

public class RedisSink extends RichSinkFunction<CityAqiResult>{

    private transient Jedis jedis;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        jedis = new Jedis("localhost", 6379);
        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(CityAqiResult value, Context context) throws com.fasterxml.jackson.core.JsonProcessingException {

        String city = value.getCity();
        int aqi = value.getAqi();
        
        String json=mapper.writeValueAsString(value);

        jedis.hset("aqi:latest", city, json);

        jedis.zadd("aqi:history:" + city, value.getWindowEndTS(), String.valueOf(aqi));

        jedis.zadd("aqi:ranking", aqi , city);

    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}

