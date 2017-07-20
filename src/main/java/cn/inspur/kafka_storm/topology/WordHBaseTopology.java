package cn.inspur.kafka_storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.inspur.kafka_storm.bolt.WordCounterBolt;
import cn.inspur.kafka_storm.bolt.WordHBaseBolt;
import cn.inspur.kafka_storm.spout.WordSpout;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by pingfuli on 2017/7/2.
 */
public class WordHBaseTopology {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";

    public static void main(String[] args) throws Exception {
        //设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数（6个）
        String zkConnString = "datanode1.bigdata:2181,manager.bigdata:2181,master2.bigdata:2181";
        String topic = "test";
        String groupId = "id";
        ZkHosts zkHosts = new ZkHosts(zkConnString);//kafka所在的zookeeper
        System.out.println("groupId = " + groupId);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/"+topic, groupId);  // create /test id
        spoutConfig.ignoreZkOffsets = true;
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


//        WordSpout spout = new WordSpout();
        WordCounterBolt bolt = new WordCounterBolt();
        WordHBaseBolt hbase = new WordHBaseBolt();

        // WordSpout ==> WordCountBolt ==> WordHBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, kafkaSpout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, hbase, 10).fieldsGrouping(COUNT_BOLT, new Fields("word"));

//        kafkaSpout.ack("");

        Config config = new Config();
        config.setDebug(true);

        if (args.length == 0) {
            System.out.println("I'm in local topology ... begin");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word", config, builder.createTopology());
            Thread.sleep(5000);
//            cluster.killTopology("word");
//            cluster.shutdown();
            System.out.println("I'm in local topology ... end");
        } else {
            System.out.println("I'm in cluster topology ...");
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
