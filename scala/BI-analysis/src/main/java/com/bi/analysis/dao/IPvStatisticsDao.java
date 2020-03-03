package com.bi.analysis.dao;

import com.bi.analysis.domain.PvStatistics;

import java.util.List;

/**
 * Created by Administrator on 2017/12/15.
 */
public interface IPvStatisticsDao {
    void updateBatch(List<PvStatistics> pvStat);
}
