package com.bi.analysis.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.bi.analysis.conf.ConfigurationManager;
import com.bi.analysis.constant.Constant;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Administrator on 2017/12/20.
 */
public class DBPoolConnection {

    private static DBPoolConnection dbPoolConnection = null;
    private static DruidDataSource datasource = null;
    private static Properties dbprop = new Properties();
    static {
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("db_server.properties");
        try {
            dbprop.load(in);
            datasource = (DruidDataSource) DruidDataSourceFactory.createDataSource(dbprop);
        } catch (Exception e) {
            e.printStackTrace();
        }

//        String driver = ConfigurationManager.getProperty(Constant.JDBC_DRIVER);
//        String url = ConfigurationManager.getProperty(Constant.JDBC_URL);
//        String user = ConfigurationManager.getProperty(Constant.JDBC_USER);
//        String password = ConfigurationManager.getProperty(Constant.JDBC_PASSWORD);
//        String maxActive = ConfigurationManager.getProperty(Constant.JDBC_MAXACTIVE);
//        String initialSize = ConfigurationManager.getProperty(Constant.JDBC_INITIALSIZE);
//        String minIdle = ConfigurationManager.getProperty(Constant.JDBC_MINIDLE);
//        String timeBetweenEvictionRunsMillis = ConfigurationManager.getProperty(Constant.JDBC_TIMEBETWEENEVICTIONRUNSMILLIS);
//        String minEvictableIdleTimeMillis = ConfigurationManager.getProperty(Constant.JDBC_MINEVICTABLEIDIETIMEMILLIS);
//        String validationQuery = ConfigurationManager.getProperty(Constant.JDBC_VALIDATIONQUERY);
//        String testWhileIdle = ConfigurationManager.getProperty(Constant.JDBC_TESTWHILEIDLE);
//        String testOnBorrow = ConfigurationManager.getProperty(Constant.JDBC_TESTONBORROW);
//        String testOnReturn = ConfigurationManager.getProperty(Constant.JDBC_TESTONRETURN);
//        String maxWait = ConfigurationManager.getProperty(Constant.JDBC_MAXWAIT);
//        datasource = new DruidDataSource();
//        datasource.setDriverClassName(driver);
//        datasource.setUrl(url);
//        datasource.setUsername(user);
//        datasource.setPassword(password);
//        datasource.setMaxActive(Integer.parseInt(maxActive));
//        datasource.setMinIdle(Integer.parseInt(minIdle));
//        datasource.setInitialSize(Integer.parseInt(initialSize));
//        datasource.setTimeBetweenConnectErrorMillis(Long.valueOf(timeBetweenEvictionRunsMillis));
//        datasource.setMinEvictableIdleTimeMillis(Long.valueOf(minEvictableIdleTimeMillis));
//        datasource.setValidationQuery(validationQuery);
//        datasource.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
//        datasource.setTestOnReturn(Boolean.parseBoolean(testOnReturn));
//        datasource.setTestWhileIdle(Boolean.parseBoolean(testWhileIdle));
//        datasource.setMaxWait(Long.valueOf(maxWait));
//        datasource.setUseGlobalDataSourceStat(true);
//        try {
//            datasource.setFilters("stat");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * 数据库连接池单例
     * @return
     */
    public static synchronized DBPoolConnection getInstance(){
        if(dbPoolConnection == null) {
            synchronized(DBPoolConnection.class) {
                if(dbPoolConnection == null) {
                    dbPoolConnection = new DBPoolConnection();
                }
            }
        }
        return dbPoolConnection;
    }

    private DBPoolConnection(){}

    /**
     * 返回druid数据库连接
     * @return
     * @throws SQLException
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return datasource.getConnection();
    }

}
