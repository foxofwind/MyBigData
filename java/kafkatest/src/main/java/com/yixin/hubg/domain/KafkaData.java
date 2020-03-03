package com.yixin.hubg.domain;

import lombok.Data;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
public class KafkaData<T> implements Serializable {
    private String id;
    private String type;
    private Long ts;
    private String table;
    private String database;
    private List<T> data;

}
