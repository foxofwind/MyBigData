package com.bi.analysis.test;




import com.bi.analysis.constant.Constant;
import com.bi.analysis.dao.IPvStatisticsDao;
import com.bi.analysis.dao.factory.DaoFactory;
import com.bi.analysis.jdbc.JDBCHelper;
import com.bi.analysis.util.DateUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/12/13.
 */
public class Test {
    private LinkedList<String> datasource = new LinkedList<String>();
    public static void main(String[] args) {

        try {
            JDBCHelper jdbcHelper = JDBCHelper.getInstance();
//            String selectSQL = "INSERT INTO p_topic_branch(id,topic_id,brch_order,is_answer,brch_content) values(?,?,?,?,?) ";
//            String selectSQL = "UPDATE p_topic_branch set is_answer=? where id=? ";
            String selectSQL = "SELECT * FROM test where id=1";
            Object[] params = new Object[]{1};
            jdbcHelper.executeQuery(selectSQL, null, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()) {
                        int count = rs.getInt(1);
                        int num = rs.getInt(2);
                        System.out.println(count);
                        System.out.println(num);
                        System.out.println(rs.getString(5));
                    }
                }

            });
//            jdbcHelper.executeUpdate(selectSQL,params);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeToFileZipFileContents(String zipFileName,
                                                  String outputFileName)
            throws java.io

            .IOException {

        java.nio.charset.Charset charset =
                java.nio.charset.StandardCharsets.US_ASCII;
        java.nio.file.Path outputFilePath =
                java.nio.file.Paths.get(outputFileName);

        // Open zip file and create output file with
        // try-with-resources statement

        try (
                java.util.zip.ZipFile zf =
                        new java.util.zip.ZipFile(zipFileName);
                java.io

                        .BufferedWriter writer =
                        java.nio.file.Files.newBufferedWriter(outputFilePath, charset)
        ) {
            // Enumerate each entry
            for (java.util.Enumeration entries =
                 zf.entries(); entries.hasMoreElements();) {
                // Get the entry name and write it to the output file
                String newLine = System.getProperty("line.separator");
                String zipEntryName =
                        ((java.util.zip.ZipEntry)entries.nextElement()).getName() +
                                newLine;
                writer.write(zipEntryName, 0, zipEntryName.length());
            }
        }
    }


    public static void test(int dbType, int times) throws Exception {
        int numOfThreads = Runtime.getRuntime().availableProcessors() * 2;
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        boolean createResult = false;
//        try {
//            test.createTable();
//            createResult = true;
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        if (createResult) {
//            List<Future<Long>> results = new ArrayList<Future<Long>>();
//            for (int i = 0; i < times; i++) {
//                results.add(executor.submit(new Callable<Long>() {
//                    @Override
//                    public Long call() throws Exception {
//                        long begin = System.currentTimeMillis();
//                        try {
//                            test.insert();
//                            // insertResult = true;
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        long end = System.currentTimeMillis();
//                        return end - begin;
//                    }
//                }));
//            }
//            executor.shutdown();
//            while (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS))
//                ;
//
//            long sum = 0;
//            for (Future<Long> result : results) {
//                sum += result.get();
//            }
//
//            System.out.println("---------------db type " + dbType
//                    + "------------------");
//            System.out.println("number of threads :" + numOfThreads + " times:"
//                    + times);
//            System.out.println("running time: " + sum + "ms");
//            System.out.println("TPS: " + (double) (100000 * 1000)
//                    / (double) (sum));
//            System.out.println();
//            try {
//                //                test.tearDown();
//                // dropResult = true;
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("初始化数据库失败");
//        }

    }
}
