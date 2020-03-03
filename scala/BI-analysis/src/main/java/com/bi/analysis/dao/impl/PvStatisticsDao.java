package com.bi.analysis.dao.impl;

import com.bi.analysis.dao.IPvStatisticsDao;
import com.bi.analysis.domain.PvStatistics;
import com.bi.analysis.jdbc.JDBCHelper;
import com.bi.analysis.module.PvStatQueryResult;
import com.bi.analysis.util.DateUtils;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/12/15.
 */
public class PvStatisticsDao implements IPvStatisticsDao{

    @Override
    public void updateBatch(List<PvStatistics> pvStats) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        List<PvStatistics> insertPvStatisticss = new ArrayList<PvStatistics>();
        List<PvStatistics> updatePvStatisticss = new ArrayList<PvStatistics>();

//        StringBuffer whereSQl = new StringBuffer("WHERE project_name=? AND create_time=? ");
//        if(Constant.PV_TYPE_1.equals(type)){
//            whereSQl.append("AND path=?");
//        }

        String selectSQL = "SELECT * FROM pv_statistics WHERE project_name=? AND create_time=?";
        for(PvStatistics pvStatistics : pvStats) {

//            if(Constant.PV_TYPE_1.equals(type)){
//                params = new Object[]{pvStatistics.getProjectName(),
//                        pvStatistics.getCreateTime(),
//                        pvStatistics.getPath()
//                };
//            }

            final PvStatQueryResult queryResult = new PvStatQueryResult();
            Object[] params = new Object[]{pvStatistics.getProjectName(),pvStatistics.getCreateTime()};
            jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()) {
                        int count = rs.getInt(5);
                        queryResult.setCount(count);
                    }
                    if(rs!=null){
                        rs.close();
                    }
                }
            });
            int count = queryResult.getCount();
            if(count > 0) {
                pvStatistics.setPvCount(pvStatistics.getPvCount() + count);
                updatePvStatisticss.add(pvStatistics);
            } else {
                insertPvStatisticss.add(pvStatistics);
            }
        }

        // 对于需要插入的数据，执行批量插入操作
        if(!insertPvStatisticss.isEmpty()){
            String insertSQL = "INSERT INTO pv_statistics(project_name,path,create_time,pv_count,type) VALUES(?,?,?,?,?)";
            List<Object[]> insertParamsList = new ArrayList<Object[]>();
            for(PvStatistics PvStatistics : insertPvStatisticss) {
                Object[] params = new Object[]{PvStatistics.getProjectName(),
                        PvStatistics.getPath(),
                        PvStatistics.getCreateTime(),
                        PvStatistics.getPvCount(),
                        "0"};
                insertParamsList.add(params);
            }
            jdbcHelper.executeBatch(insertSQL, insertParamsList);
        }

        // 对于需要更新的数据，执行批量更新操作
        if(!updatePvStatisticss.isEmpty()){

            String updateSQL = "UPDATE pv_statistics SET pv_count=? WHERE project_name=? AND create_time=? ";
            for(PvStatistics PvStatistics : updatePvStatisticss) {
                Object[] params = new Object[]{ PvStatistics.getPvCount(),
                        PvStatistics.getProjectName(),
                        PvStatistics.getCreateTime()
                };
                jdbcHelper.executeUpdate(updateSQL,params);
            }
        }
//            List<Object[]> updateParamsList = new ArrayList<Object[]>();
//                if(Constant.PV_TYPE_1.equals(type)){
//                    params = new Object[]{ PvStatistics.getPvCount(),
//                            PvStatistics.getProjectName(),
//                            PvStatistics.getCreateTime(),
//                            PvStatistics.getPath()
//                    };
//                }else{
//                }
//                updateParamsList.add(params);
//            jdbcHelper.execsuteBatch(updateSQL, updateParamsList);
    }
}
