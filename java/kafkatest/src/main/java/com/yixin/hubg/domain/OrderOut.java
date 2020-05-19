package com.yixin.hubg.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Create By 鸣宇淳 on 2020/2/10
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderOut implements Serializable {
    private String sqbh;
    private Integer oMin;
    private Integer oMax;
    private Integer oSum;
    private Integer oFirst;
}
