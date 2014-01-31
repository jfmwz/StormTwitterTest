package com.cap3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.cap3.Boult.GeocodeLookup;
import com.cap3.Boult.HeatMapBuilder;
import com.cap3.Boult.Persistor;
import com.cap3.Spout.Checkins;


public class LocalTopologyRunner {
  public static void main(String[] args) {
    Config config = new Config();
    config.setDebug(true);

    LocalCluster localCluster = new LocalCluster();
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("checkins", new Checkins(),4);
    builder.setBolt("geocode-lookup", new GeocodeLookup(),8).setNumTasks(64)
        .shuffleGrouping("checkins");
    builder.setBolt("heatmap-builder", new HeatMapBuilder())
        .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3)
        .globalGrouping("geocode-lookup");
    builder.setBolt("persistor", new Persistor())
        .shuffleGrouping("heatmap-builder");

    localCluster.submitTopology("local-heatmap", config, builder.createTopology());

  }
}