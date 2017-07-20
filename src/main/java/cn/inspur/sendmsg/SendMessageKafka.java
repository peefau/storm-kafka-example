package cn.inspur.sendmsg;

/**
 * Created by whoami on 2016/11/24.
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static java.lang.Thread.sleep;

public class SendMessageKafka {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "master2.bigdata:6667,datanode1.bigdata:6667,datanode2.bigdata:6667");
//        props.put("bootstrap.servers", "master2.bigdata:6667"); //经测试，只要消费者指定zookeeper地址和端口，这里写一个IP或多个IP都是可以的
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Random r = new Random();

        while(true)
        {
            try {
                int id = r.nextInt(10000000);
                int memberid = r.nextInt(100000);
                int totalprice = r.nextInt(1000) + 100;
                int preferential = r.nextInt(100);
                int sendpay = r.nextInt(3);

                StringBuffer data = new StringBuffer();
                data.append(String.valueOf(id)).append("\t")
                        .append(String.valueOf(memberid)).append("\t")
                        .append(String.valueOf(totalprice)).append("\t")
                        .append(String.valueOf(preferential)).append("\t")
                        .append(String.valueOf(sendpay)).append("\t")
                        .append(df.format(new Date()));
                System.out.println(data.toString());

                producer.send(new ProducerRecord<String, String>("test", Integer.toString(id), data.toString()));
                sleep(1000);
            } catch (Exception e) {
                producer.close();
                e.printStackTrace();
                break;
            }
        }

    }

}
