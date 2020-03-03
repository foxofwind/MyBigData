package com.yixin.dw.flink.domain;

import lombok.Data;

/**
 * Create By 鸣宇淳 on 2020/2/18
 **/
@Data
public class KafkaEntiesBase {
    private Long id;
    private String type;
    private Long ts;
    private String table;
    private String database;
}
