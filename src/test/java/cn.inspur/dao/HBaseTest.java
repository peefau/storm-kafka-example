package cn.inspur.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;

/**
 * Created by pingfuli on 2017/7/3.
 */
public class HBaseTest {


    public static void main(String[] args) {

        final Random random = new Random();
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.rootdir","hdfs://master2.bigdata:8020/apps/hbase/data");
        config.set("hbase.zookeeper.quorum","master2.bigdata:2181,datanode1.bigdata:2181,manager.bigdata:2181");
        config.set("zookeeper.znode.parent","/hbase-unsecure");
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            //示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
            Table table = connection.getTable(TableName.valueOf("t_word_count"));

            Put p = new Put(Bytes.toBytes("myRowKey"));
            p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("word"),Bytes.toBytes("I"));
            p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("word"),Bytes.toBytes("like"));
            p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("word"),Bytes.toBytes("Storm"));
            p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("word"),Bytes.toBytes("!!!"));
            table.put(p);
            System.out.println(random.nextInt(8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
