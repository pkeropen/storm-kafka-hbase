package com.vita;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Map;


public class Topohogy {
    static Logger logger = LoggerFactory.getLogger(Topohogy.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {

        String topic = "test";
        String zkRoot = "";
        String id = "old";
        BrokerHosts brokerHosts = new ZkHosts("zk:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
//        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("word-spilter", new SpliterBolt()).shuffleGrouping("KafkaSpout");
        builder.setBolt("writer", new CountBolt(), 1).fieldsGrouping("word-spilter", new Fields("word"));

        System.out.println("builder");

        SimpleHBaseMapper mapper = new SimpleHBaseMapper();

        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir", "hdfs://hadoop:9000/hbase");
        hbConf.put("zookeeper.znode.parent", "/hbase");
        hbConf.put("hbase.zookeeper.quorum", "zk:2181");

        //wordcount为表名
        HBaseBolt hBaseBolt = new HBaseBolt("wordcount", mapper).withConfigKey("hbase.conf");
        //result为列族名
        mapper.withColumnFamily("result");
        mapper.withColumnFields(new Fields("count"));
        mapper.withRowKeyField("word");

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(0);
        conf.setDebug(true);
        conf.put("hbase.conf", hbConf);

        // hbase-bolt
        builder.setBolt("hbase", hBaseBolt, 1).shuffleGrouping("writer");

        if (args != null && args.length > 0) {
            //提交topology到storm集群中运行
            StormSubmitter.submitTopology("sufei-topo", conf, builder.createTopology());
        } else {
            //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCount", conf, builder.createTopology());
        }
    }
}

