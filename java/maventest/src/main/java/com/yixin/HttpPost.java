package com.yixin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

public class HttpPost {

    public static void main(String[] args) {
        String httpUrl = "http://192.168.177.104:8090/hydra-api/api/watermark/makeTransparentWatermark";
        String param = "{                                                            "
                +"        \"waterMarkContent\": \"易鑫大数据-BI-附件导出项目\","
                +"        \"isShowMD5\": 1,                                    "
                +"        \"isLightOrShade\": 1,                               "
                +"        \"isOnlyPushData\": 0,                               "
                +"        \"heigth\": 400,                                     "
                +"        \"width\": 400,                                      "
                +"        \"color\": \"#ff0000\",                              "
                +"        \"degree\": -20,                                     "
                +"        \"alpha\": 0.3,                                      "
                +"        \"fontSize\": 30,                                    "
                +"        \"platformCode\": \"wfpt\",                          "
                +"        \"sensitiveDataMap\":                                "
                +"            {                                                "
                +"                \"ip\": \"192.168.145.39\",                  "
                +"                \"message\": \"任意敏感数据\",               "
                +"                \"requst\": \"请求内容\",                    "
                +"                \"time\": \"2019-11-05 19:38:12\",           "
                +"                \"userId\": \"用户域账号\",                  "
                +"                \"userName\": \"用户名称\",                  "
                +"                \"phone\": \"联系电话\",                     "
                +"                \"email\": \"邮件\",                         "
                +"                \"domain\": \"yidaitong.yxqiche.com\",       "
                +"                \"department\": \"部门\"                     "
                +"            }                                                "
                +"    }                                                        ";
        String ret = doPost(httpUrl,param);
        System.out.println(ret);
        JSONObject json = JSON.parseObject(ret);
        base64ToFile(json.getJSONObject("data").getString("info"),"water.png");
    }

    public static void base64ToFile(String base64, String fileName) {
        File file = null;
        //创建文件目录
        String filePath="D:\\image";
        File  dir=new File(filePath);
        if (!dir.exists() && !dir.isDirectory()) {
            dir.mkdirs();
        }
        BufferedOutputStream bos = null;
        java.io.FileOutputStream fos = null;
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            file=new File(filePath+"\\"+fileName);
            fos = new java.io.FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param httpUrl 发送请求的 URL
     * @param param   请求参数是json
     * @return 所代表远程资源的响应结果
     */
    public static String doPost(String httpUrl, String param) {


        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            // 设置连接主机服务器超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取主机服务器返回数据超时时间：60000毫秒
            connection.setReadTimeout(60000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/json");
            // 设置鉴权信息：Authorization: Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0
            // connection.setRequestProperty("Authorization", "Bearer
            // da3efcbf-0845-4fe3-8aba-ee040be542c0");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
        }
        return result;
    }
}
