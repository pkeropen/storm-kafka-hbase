package com.vita;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;


public class CountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    private FileWriter writer = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String word = tuple.getString(0);

        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }

        count++;

        counts.put(word, count);
        System.out.println("hello word!");
        System.out.println(word + "  " + count);

        collector.emit(new Values(word, count));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
