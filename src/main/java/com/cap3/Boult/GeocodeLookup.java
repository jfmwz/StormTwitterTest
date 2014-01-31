package com.cap3.Boult;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class GeocodeLookup extends BaseBasicBolt {
    private Geocoder geocoder;
    private Jedis jedis;
    private ObjectMapper objectMapper;

    private final Logger logger = LoggerFactory.getLogger(GeocodeLookup.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
        fieldsDeclarer.declare(new Fields("time", "geocode"));
    }

    @Override
    public void prepare(Map stormConf,
                        TopologyContext context) {
        geocoder = new Geocoder();
        jedis = new Jedis("192.168.116.200");
        objectMapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple tuple,
                        BasicOutputCollector outputCollector) {
        String address = tuple.getStringByField("address");
        Long time = tuple.getLongByField("time");

        // try if exists en local database
        try {
            String key = address;
            String value = jedis.get(key);
            if (value == null) {
                GeocoderRequest request = new GeocoderRequestBuilder()
                        .setAddress(address)
                        .setLanguage("en")
                        .getGeocoderRequest();
                GeocodeResponse response = geocoder.geocode(request);
                GeocoderStatus status = response.getStatus();
                if (GeocoderStatus.OK.equals(status)) {
                    GeocoderResult firstResult = response.getResults().get(0);
                    LatLng latLng = firstResult.getGeometry().getLocation();

                    value = objectMapper.writeValueAsString(latLng);
                    jedis.set(key, value);

                    outputCollector.emit(new Values(time, latLng));
                }
            }   else {

            }

        } catch (Exception e) {
            logger.error("Error get key: ", e);
        }

    }
}
