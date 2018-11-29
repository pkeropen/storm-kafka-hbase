package com.vita;
/**
 * @Title: SpliterBolt.java
 * @Package com.vita
 * @Description: TODO(用一句话描述该文件做什么)
 * @author caiz
 * @date 2018/11/26 14:25
 * @version v.3.0
 */


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.StringTokenizer;

/**
 * @ClassName: SpliterBolt
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author caiz
 * @date 2018/11/26 14:25 
 *
 */
public class SpliterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector){

        String sentence = tuple.getString(0);

        StringTokenizer iter = new StringTokenizer(sentence);

        while(iter.hasMoreElements()){
            collector.emit(new Values(iter.nextToken()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("word"));
    }
}

