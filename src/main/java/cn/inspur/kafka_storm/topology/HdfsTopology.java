package cn.inspur.kafka_storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import cn.inspur.kafka_storm.bolt.CheckOrderBolt;
import cn.inspur.kafka_storm.bolt.CounterBolt;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;

import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Created by whoami on 2016/11/26.
 * https://github.com/apache/storm/tree/master/external/storm-hdfs
 */
public class HdfsTopology {
    public static void main(String[] args) {
        try {
            String zkhost = "datanode1.bigdata:2181,manager.bigdata:2181,master2.bigdata:2181";
            String topic = "test";
            String groupId = "id";
            int spoutNum = 3;
            int boltNum = 1;
            ZkHosts zkHosts = new ZkHosts(zkhost);//kafka所在的zookeeper
            SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/test", groupId);  // create /order /id
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

            // HDFS bolt
            // use "|" instead of "," for field delimiter
            RecordFormat format = new DelimitedRecordFormat()
                    .withFieldDelimiter("|");

            // sync the filesystem after every 1k tuples
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);

            // rotate files when they reach 5MB
            FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
            // FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

            FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                    .withPath("/tmp/").withPrefix("test_").withExtension(".log");

            HdfsBolt hdfsBolt = new HdfsBolt()
                    .withFsUrl("hdfs://master2.bigdata:8020")
                    .withFileNameFormat(fileNameFormat)
                    .withRecordFormat(format)
                    .withRotationPolicy(rotationPolicy)
                    .withSyncPolicy(syncPolicy);

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout", kafkaSpout, spoutNum);
            builder.setBolt("check", new CheckOrderBolt(), boltNum).shuffleGrouping("spout");
            builder.setBolt("counter", new CounterBolt(), boltNum).shuffleGrouping("check");
            builder.setBolt("hdfs", hdfsBolt, boltNum).shuffleGrouping("counter");

            Config config = new Config();
            config.setDebug(true);

            if(args!=null && args.length > 0)
            {
                config.setNumWorkers(2);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } else {
                config.setMaxTaskParallelism(2);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("Wordcount-Topology", config, builder.createTopology());

                Thread.sleep(500000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
