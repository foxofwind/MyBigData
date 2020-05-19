package com.yixin.hubg.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Simple to Introduction
 * className: StatisticsDeserializer
 *
 * @author EricYang
 * @version 2019/3/9 11:45
 */
@Slf4j
public class OrderInDeserializer implements Deserializer<OrderIn> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public OrderIn deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        else {
            try {
                return jsonMapper.readValue(bytes, OrderIn.class);
            }
            catch (Exception ex){
                log.error("jsonSerialize exception.", ex);
                return null;
            }
        }
    }

    @Override
    public void close() {

    }
}