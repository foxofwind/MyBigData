package com.yixin.dw.flink.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
public  class DataContainer<T> implements Serializable {
    private T data;
    private T old;
    private String dbDMLType;
    private String key;
}
