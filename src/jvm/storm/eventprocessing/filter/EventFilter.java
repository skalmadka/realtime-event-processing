package storm.eventprocessing.filter;


import storm.eventprocessing.EventProcessingConfig;
import storm.eventprocessing.filter.bloomfilter.RedisBloomFilter;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import java.util.Map;

/**
 * Created by Sunil Kalmadka on 4/26/2015.
 */

public class EventFilter extends BaseFilter {
    private RedisBloomFilter<String> bloomFilter;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        bloomFilter = new RedisBloomFilter<String>(Integer.parseInt(conf.get(EventProcessingConfig.BLOOM_FILTER_EXPECTED_ELEMENT_COUNT).toString()),
                                                    Double.parseDouble(conf.get(EventProcessingConfig.BLOOM_FILTER_DESIRED_FALSE_POSITIVE).toString()),
                                                    conf.get(EventProcessingConfig.REDIS_HOST_NAME).toString(),
                                                    Short.parseShort(conf.get(EventProcessingConfig.REDIS_HOST_PORT).toString()),
                                                    conf.get(EventProcessingConfig.BLOOM_FILTER_NAME).toString()
                                                    );
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String eventType = tridentTuple.getString(0);
        String url = tridentTuple.getString(1);
        String description = tridentTuple.getString(2);
        String task = tridentTuple.getString(3);
        String crawl_depth = tridentTuple.getString(4);

        if(EventProcessingConfig.EVENT_TYPE_FILE.equals(eventType)){
            return true;    //Allow all file events
        }

        if (bloomFilter.exists(url+task)) {
            System.out.println("----- BloomFilter reject (URL already tagged for this task):" + url+task);
            return false;
        }
        bloomFilter.add(url);
        return true;
    }
}
