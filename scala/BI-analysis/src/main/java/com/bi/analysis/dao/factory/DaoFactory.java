package com.bi.analysis.dao.factory;

import com.bi.analysis.dao.IPvStatisticsDao;
import com.bi.analysis.dao.impl.PvStatisticsDao;

/**
 * Created by Administrator on 2017/12/15.
 */
public class DaoFactory {
    public static IPvStatisticsDao getPvStatisticsDao(){return new PvStatisticsDao();}
}
