package com.bi.analysis;

import com.bi.analysis.conf.ConfigurationManager;
import com.bi.analysis.constant.Constant;
import com.bi.analysis.dao.IPvStatisticsDao;
import com.bi.analysis.dao.factory.DaoFactory;
import com.bi.analysis.domain.Log;
import com.bi.analysis.domain.PvStatistics;
import com.bi.analysis.util.DateUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;

import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Administrator on 2017/12/7.
 */
public class PvCountRealTimeStat {

    public static void main(String[] args) {

        try {

//            while (true){
                JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
                        ConfigurationManager.getProperty(Constant.CHECK_POINT_URL),
                        new Function0<JavaStreamingContext>() {
                            @Override
                            public JavaStreamingContext call() throws Exception {
                                SparkConf conf = new SparkConf()
                                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                        .setAppName("PvCountRealTimeStat");
                                JavaStreamingContext sc = new JavaStreamingContext(conf,Durations.minutes(1));
                                sc.checkpoint(ConfigurationManager.getProperty(Constant.CHECK_POINT_URL));
                                /**
                                 * 创建kafka数据来源流
                                 */
                                String topic = ConfigurationManager.getProperty(Constant.KAFKA_TOPICS);
                                String[] topics = topic.split(",");

                                Set<String> topicSet = new HashSet<String>();
                                for(int i=0;i<topics.length;i++){
                                    topicSet.add(topics[i]);
                                }
                                Map<String, Object> kafkaParams = new HashMap<String, Object>();
                                kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constant.KAFKA_METADATA_BROKER_LIST));
                                kafkaParams.put("key.deserializer", StringDeserializer.class);
                                kafkaParams.put("value.deserializer", StringDeserializer.class);
                                kafkaParams.put("group.id", ConfigurationManager.getProperty(Constant.KAFKA_GROUP));
                                kafkaParams.put("auto.offset.reset", "latest");
                                kafkaParams.put("enable.auto.commit", false);

                                JavaInputDStream<ConsumerRecord<String, String>> logDStream =
                                        KafkaUtils.createDirectStream(
                                                sc,
                                                LocationStrategies.PreferConsistent(),
                                                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                                        );
                                if (logDStream != null) {
                                    //topic|domain|logtime|path
                                    JavaDStream<String> lines = getLogActionDStream(logDStream);

                                    //计算日志总PV数
                                    JavaPairDStream<String, Long> pvLogStatDStream = calculatePvLogState(lines);

                                    //计算pathPV数
//                                   JavaPairDStream<String, Long> pvPathStatDStream = calculatePvPathState(lines);
                                }
                                return sc;
                            }
                        });

                jssc.start();
                jssc.awaitTermination();
//                jssc.awaitTerminationOrTimeout(DateUtils.getResetTime());
//                jssc.stop(true,true);
//            }

//            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
//            jssc.checkpoint(ConfigurationManager.getProperty(Constant.CHECK_POINT_URL));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 拼接log信息
     * @param logDStream
     * @return topic|domain|logtime|path
     */
    private static JavaDStream<String> getLogActionDStream(JavaInputDStream<ConsumerRecord<String, String>> logDStream) {

        if(logDStream == null){
            return  null;
        }
        JavaDStream<String>  line = logDStream.mapPartitions(
                new FlatMapFunction<Iterator<ConsumerRecord<String, String>>, String>() {
                @Override
                public Iterator<String> call(Iterator<ConsumerRecord<String, String>> iterator) throws Exception {
                    List<String> lines = new LinkedList<String>();
                    while (iterator.hasNext()){
                        ConsumerRecord<String, String> record = iterator.next();
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
                        Log log =  mapper.readValue(record.value(),Log.class);
                        String result = record.topic() + "|" + DateUtils.formatTimeMinute(log.getLogtime());
                        lines.add(result);
                    }
                    return lines.iterator();
                }
        });
        return line;

//        JavaDStream<String> lines = logDStream.map(new Function<ConsumerRecord<String, String>, String>() {
//            @Override
//            public String call(ConsumerRecord<String, String> record) throws Exception {
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//                Log log =  mapper.readValue(record.value(),Log.class);
//                String domain = StringUtils.isNotBlank(log.getDomain()) ? log.getDomain() : Constant.DOMAIN;
//                String path = log.getPath().contains("?") ? log.getPath().split("//?")[0] : log.getPath();
//                String domainPath = domain + path;
//                if(domainPath.endsWith("/")){
//                    domainPath = domainPath.substring(0,domainPath.length()-1);
//                }
//                String result = record.topic() + "|"
//                        + domain+"|"
//                        + DateUtils.formatTimeMinute(log.getLogtime());
//                return result;
//            }
//        });
//        .filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String line) throws Exception {
//                String[] lines = line.split("\\|");
//                String path = lines[3];
//                if(path.length() >= ConfigurationManager.getInteger(Constant.PATH_LENGTH)){
//                    return false;
//                }
//                return true;
//            }
//        });
    }

    /**
     * 计算日志总PV数
     * @param lines
     * @return
     */
    private static JavaPairDStream<String,Long> calculatePvLogState(JavaDStream<String> lines) {

        if(lines == null){
            return null;
        }

        //对原始数据进行map映射，<topic_logtime,1>
        JavaPairDStream<String,Long> mappedDStream = lines.mapToPair(
                new PairFunction<String, String, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Long> call(String line) throws Exception {
                        String[] lines = line.split("\\|");
                        String topic = lines[0];
                        String logtime = lines[1];
                        String key = topic + "|" + logtime;
                        return new Tuple2<String, Long>(key,1L);
                    }
                });

        JavaPairDStream<String,Long> aggregatedcounts = mappedDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
        });

        //
        aggregatedcounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                if(!rdd.isEmpty()){
                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                            List<PvStatistics> pvStats = new ArrayList<PvStatistics>();
                            while (iterator.hasNext()){
                                Tuple2<String,Long> tuple = iterator.next();
                                String[] keySplited = tuple._1.split("\\|");
                                PvStatistics pvStatistics = new PvStatistics();
                                pvStatistics.setProjectName(keySplited[0]);
                                pvStatistics.setCreateTime(DateUtils.dateToStamp(keySplited[1]));
                                pvStatistics.setPvCount(tuple._2.intValue());
                                pvStatistics.setPath("topic");
                                pvStats.add(pvStatistics);
                            }
                            IPvStatisticsDao pvStatisticsDao = DaoFactory.getPvStatisticsDao();
                            pvStatisticsDao.updateBatch(pvStats);
                        }
                    });
                }
            }
        });

        //HDFS维护一份全局key
        JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> aggregatedDStream = mappedDStream.mapWithState(StateSpec.function(
                new Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Long> call(String key, Optional<Long> optional, State<Long> state) throws Exception {
                        Long count = optional.get() + (state.exists() ? state.getOption().get() : 0);
                        Tuple2<String,Long> tuple = new Tuple2<>(key,count);
                        state.update(count);
                        return tuple;
                    }
                }
        ));
        aggregatedDStream.checkpoint(Durations.seconds(300));
//        JavaPairDStream<String,Long> aggregatedDStream= aggregatedDStream1.stateSnapshots();
        //将计算出来的最新结果，同步到mysql中

        //累加每个batch rdd
//        JavaPairDStream<String,Long> aggregatedDStream = mappedDStream.updateStateByKey(
//                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
//                @Override
//                public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
//                    long count = 0L;
//                    if(optional.isPresent()){
//                        count = optional.get();
//                    }
//                    for(Long value : values){
//                        count += value;
//                    }
//                    return Optional.of(count);
//                }
//            });

        return aggregatedcounts;


    }

    /**
     * 计算path PV数
     * @param lines
     * @return
     */
//    private static JavaPairDStream<String,Long> calculatePvPathState(JavaDStream<String> lines) {
//
//        if (lines == null){
//            return null;
//        }
//
//        //对原始数据进行map映射，<topic_logtime,1>
//        JavaPairDStream<String,Long> mappedDStream = lines.mapToPair(
//                new PairFunction<String, String, Long>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public Tuple2<String, Long> call(String line) throws Exception {
//                        String[] lines = line.split("\\|");
//                        String topic = lines[0];
//                        String path = lines[3];
//                        String logtime = lines[2];
//                        String key = topic + "|" +  path + "|" + logtime;
//                        return new Tuple2<String, Long>(key,1L);
//                    }
//                });
//
//
//        Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>> mappingFunction1 =
//                new Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public Tuple2<String, Long> call(String key, Optional<Long> optional, State<Long> state) throws Exception {
//                        Long count = optional.get() + (state.exists() ? state.getOption().get() : 0);
//                        Tuple2<String,Long> tuple = new Tuple2<>(key,count);
//                        state.update(count);
//                        return tuple;
//                    }
//                };
//
//        mappedDStream.checkpoint(Durations.seconds(150));
//        //累加batchRDD
//        JavaPairDStream<String,Long>  countMappedDStream =
//                mappedDStream.mapWithState(StateSpec.function(mappingFunction1)).stateSnapshots();
//
//        countMappedDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
//            @Override
//            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
//                if(!rdd.isEmpty()){
//                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
//                        @Override
//                        public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
//                            List<PvStatistics> pvStats = new ArrayList<PvStatistics>();
//                            while (iterator.hasNext()){
//                                Tuple2<String,Long> tuple = iterator.next();
//                                String[] keySplited = tuple._1.split("\\|");
//                                PvStatistics pvStatistics = new PvStatistics();
//                                pvStatistics.setProjectName(keySplited[0]);
//                                pvStatistics.setPath(keySplited[1]);
//                                pvStatistics.setCreateTime(DateUtils.dateToStamp(keySplited[2]));
//                                pvStatistics.setPvCount(tuple._2.intValue());
//                                pvStats.add(pvStatistics);
//                            }
//                            IPvStatisticsDao pvStatisticsDao = DaoFactory.getPvStatisticsDao();
//                            pvStatisticsDao.updateBatch(pvStats,Constant.PV_TYPE_1);
//                        }
//                    });
//                }
//            }
//        });
//            //累加每个batch rdd
////            JavaPairDStream<String,Long> aggregatedPvDStream = mappedDStream.updateStateByKey(
////                    new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
////                        @Override
////                        public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
////                            long count = 0L;
////                            if(optional.isPresent()){
////                                count = optional.get();
////                            }
////                            for(Long value : values){
////                                count += value;
////                            }
////                            return Optional.of(count);
////                        }
////                    }
////            );
//
//        return countMappedDStream;
//    }

}
