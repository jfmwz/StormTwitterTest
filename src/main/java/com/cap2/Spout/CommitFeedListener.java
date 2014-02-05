package com.cap2.Spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Cat
 * Date: 31/01/14
 * Time: 13:03
 * To change this template use File | Settings | File Templates.
 */
public class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private List<String> commits;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }

    @Override
    public void open(Map configMap,
                     TopologyContext context,
                     SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        try {
            commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream(
                    "changelog.txt"), Charset.defaultCharset().name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        for (String commit : commits) {
            outputCollector.emit(new Values(commit));
        }
    }
}
