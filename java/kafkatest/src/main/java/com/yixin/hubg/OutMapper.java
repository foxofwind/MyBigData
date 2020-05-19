package com.yixin.hubg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.OrderIn;
import com.yixin.hubg.domain.OrderOut;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Map;

public class OutMapper implements KeyValueMapper<String, String, KeyValue<String, String>> {
    @Override
    public KeyValue<String, String> apply(String key, String value) {
        Map<Integer, OrderIn> nv = JSON.parseObject(value
                , new TypeReference<Map<Integer, OrderIn>>() {
                });
        OrderOut ret = new OrderOut("", 0, 0, 0, 0);
        ret.setSqbh(key);
        for (Map.Entry<Integer, OrderIn> entry : nv.entrySet()) {
            Integer money = entry.getValue().getMoney();
            if (ret.getOMin() == 0 || money < ret.getOMin()) {
                ret.setOMin(money);
            }
            if (money > ret.getOMax()) {
                ret.setOMax(money);
            }
            ret.setOSum(ret.getOSum() + money);
            if (entry.getValue().getPeriod() == 1)
                ret.setOFirst(money);
        }
        return new KeyValue<String, String>(key,JSON.toJSONString(ret));
    }
}
