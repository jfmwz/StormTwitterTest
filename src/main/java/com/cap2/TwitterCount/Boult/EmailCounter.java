package com.cap2.TwitterCount.Boult;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Cat
 * Date: 31/01/14
 * Time: 13:08
 * To change this template use File | Settings | File Templates.
 */
public class EmailCounter extends BaseBasicBolt {
    private Map<String, Integer> counts;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
// This bolt does not emit anything and therefore does
// not declare any output fields.
    }

    @Override
    public void prepare(Map config,
                        TopologyContext context) {
        counts = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple,
                        BasicOutputCollector outputCollector) {
        String email = tuple.getStringByField("email");
        counts.put(email, countFor(email) + 1);
        printCounts();
    }

    private Integer countFor(String email) {
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    private void printCounts() {
        for (String email : counts.keySet()) {
            System.out.println(
                    String.format("%s has count of %s", email, counts.get(email)));
        }
    }
}