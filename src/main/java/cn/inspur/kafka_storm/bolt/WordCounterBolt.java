package cn.inspur.kafka_storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by pingfuli on 2017/7/2.
 */
public class WordCounterBolt extends BaseBasicBolt{
    private Map<String,Integer> wordCountMap = new HashMap<String,Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        int count;
        if (wordCountMap.containsKey(word)){
            count = wordCountMap.get(word);
        } else {
            count = 0;
        }
        count++;
        wordCountMap.put(word,count);
        collector.emit(new Values(word,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word","count"));
    }

    @Override
    public void cleanup() {
        Iterator iter = wordCountMap.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println("key = "+key+",value = "+value);
        }
    }
}
