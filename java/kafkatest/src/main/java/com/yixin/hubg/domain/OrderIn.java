package com.yixin.hubg.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderIn implements Serializable {
    private String id;
    private String key;
    private String sqbh;
    private Integer period;
    private Integer money;
}
