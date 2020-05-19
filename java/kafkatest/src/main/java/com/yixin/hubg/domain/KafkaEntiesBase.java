package com.yixin.hubg.domain;

import lombok.Data;

/**
 * Create By 鸣宇淳 on 2020/2/18
 **/
@Data
public class KafkaEntiesBase {
    private String id;
    private String type;
    private Long ts;
    private String table;
    private String database;
}
