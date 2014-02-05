package com.cap2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.cap2.Boult.EmailCounter;
import com.cap2.Boult.EmailExtractor;
import com.cap2.Spout.CommitFeedListener;

/**
 * Created with IntelliJ IDEA.
 * User: Cat
 * Date: 31/01/14
 * Time: 13:10
 * To change this template use File | Settings | File Templates.
 */
public class LocalTopologyRunner {
    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder
                .setBolt("email-extractor", new EmailExtractor())
                .shuffleGrouping("commit-feed-listener");
        builder
                .setBolt("email-counter", new EmailCounter())
                .fieldsGrouping("email-extractor", new Fields("email"));
        Config config = new Config();
        //config.setDebug(true);

        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count-topology",
                config,
                topology);
        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();
    }
}