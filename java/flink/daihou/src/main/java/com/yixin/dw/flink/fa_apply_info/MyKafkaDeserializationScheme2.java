package com.yixin.dw.flink.fa_apply_info;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.dw.flink.domain.FaApplyInfoExtSource;
import com.yixin.dw.flink.domain.KafkaData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Create By 鸣宇淳 on 2020/2/14
 **/
public class MyKafkaDeserializationScheme2<T> implements DeserializationSchema<KafkaData<T>> {
    @Override
    public KafkaData<T> deserialize(byte[] bytes) throws IOException {
        List<T> sourceList=new ArrayList<T>();
        String jsonStr=new String(bytes, "UTF-8") ;
        KafkaData<T> kafkaData =
                JSON.parseObject(jsonStr, new TypeReference<KafkaData<T>>() {});

        return kafkaData;
    }

    @Override
    public boolean isEndOfStream(KafkaData<T> faApplyInfoExtSource) {
        return false;
    }

    @Override
    public TypeInformation<KafkaData<T>> getProducedType() {
        return TypeInformation.of(new TypeHint<KafkaData<T>>() {

        });
    }
}
