package com.bi.analysis.conf;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.bi.analysis.constant.Constant;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *  配置管理组件
 */
public class ConfigurationManager {
    private static Properties prop = new Properties();

    static {
        try {
            //用类加载器加载路径中的指定文件，获取针对指定文件的输入流
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            //将配置文件中的数据以key-value的格式加载到prop中
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key的value
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return  prop.getProperty(key);
    }

    /**
     * 获取正数类型的value
     * @param key
     * @return
     */
    public static Integer getInteger(String key){
        String value = getProperty(key);
        try{
            return  Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public static Boolean getBoolean(String key){
        String value = getProperty(key);
        try{
            return  Boolean.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static long getLong(String key){
        String value = getProperty(key);
        try{
            return  Long.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

}
