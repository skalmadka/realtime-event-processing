package storm.eventprocessing;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import storm.eventprocessing.filter.KafkaProducerFilter;
import storm.eventprocessing.filter.PrintFilter;
import storm.eventprocessing.filter.EventFilter;
import storm.eventprocessing.function.GetAdFreeWebPage;
import storm.eventprocessing.function.PrepareCrawledPageDocument;
import storm.eventprocessing.function.SplitEventString;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import storm.eventprocessing.state.ESTridentTupleMapper;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.*;
import java.lang.System;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sunil Kalmadka on 4/25/2015.
 */

public class EventProcessingTopology {
    public static StormTopology buildTopology(Config conf) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts(conf.get(EventProcessingConfig.KAFKA_CONSUMER_HOST_NAME) + ":" +conf.get(EventProcessingConfig.KAFKA_CONSUMER_HOST_PORT));
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, (String) conf.get(EventProcessingConfig.KAFKA_TOPIC_NAME));
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);

        //ElasticSearch Persistent State
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", conf.get(EventProcessingConfig.ELASTICSEARCH_CLUSTER_NAME))
                .put("storm.elasticsearch.hosts", conf.get(EventProcessingConfig.ELASTICSEARCH_HOST_NAME) + ":" + conf.get(EventProcessingConfig.ELASTICSEARCH_HOST_PORT))
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<String>(new ClientFactory.NodeClient(esSettings.getAsMap()), String.class);

/*        //Kafka State
        TridentKafkaStateFactory kafkaStateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("crawl"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("refURL", "refDepth"));

        TridentState b1 =  s.each(new Fields("href", "depth"), new PrepareHrefKafka(), new Fields("refURL", "refDepth"))
        .partitionPersist(kafkaStateFactory, new Fields("refURL", "refDepth"), new TridentKafkaUpdaterEmitTuple(), new Fields());
*/
        //Topology
        topology.newStream("crawlKafkaSpout", spout).parallelismHint(5)
                .each(new Fields("str"), new PrintFilter())
                .each(new Fields("str"), new SplitEventString(), new Fields("event_type", "event_uri", "event_desc", "event_task", "event_depth"))
                //Bloom Filter
                .each(new Fields("event_type", "event_uri", "event_desc", "event_task", "event_depth"), new EventFilter())
                //Download and Parse Webpage
                .each(new Fields("event_type", "event_uri"), new GetAdFreeWebPage(), new Fields("web_content", "web_title", "web_href"))
                //Kafka Send: Recursive Href
                .each(new Fields("event_type", "web_href", "event_depth"), new KafkaProducerFilter())
                //Insert to Elasticsearch
                .each(new Fields("event_type", "event_uri", "event_desc", "event_task", "web_content", "web_title"), new PrepareCrawledPageDocument(), new Fields("index", "type", "id", "source"))
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new ESTridentTupleMapper()), new Fields())
                ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 1){
            System.err.println("[ERROR] Configuration File Required");
        }
        Config conf = new Config();

        Map topologyConfig = readConfigFile(args[0]);
        conf.putAll(topologyConfig);

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("web_crawler", conf, buildTopology(conf));
        StormSubmitter.submitTopologyWithProgressBar("web_crawler", conf, buildTopology(conf));
    }

    private static Map readConfigFile(String filename) throws IOException {
        Map ret;
        Yaml yaml = new Yaml(new SafeConstructor());
        InputStream inputStream = new FileInputStream(new File(filename));

        try {
            ret = (Map)yaml.load(inputStream);
        } finally {
            inputStream.close();
        }

        if(ret == null) {
            ret = new HashMap();
        }

        return new HashMap(ret);
    }
}
