package com.yixin.dw.flink.domain;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
public  class KafkaData<T> extends KafkaEntiesBase implements Serializable {
    private T data;
    private T old;
    private String flag;
}
