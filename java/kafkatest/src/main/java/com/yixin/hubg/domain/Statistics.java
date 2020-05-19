package com.yixin.hubg.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Statistics {
    private Long avg;
    private Long sum;
    private Long count;
}