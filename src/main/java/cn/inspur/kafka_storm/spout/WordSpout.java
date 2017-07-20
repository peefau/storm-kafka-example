package cn.inspur.kafka_storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by pingfuli on 2017/7/2.
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final String[] MSGS = new String[]{
            "Storm", "HBase", "Integration", "example", "by ", "pingfuli", "in", "July",
    };

    private static final Random random = new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String word = MSGS[random.nextInt(8)];
        collector.emit(new Values(word));
    }
}