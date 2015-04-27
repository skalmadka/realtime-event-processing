package storm.eventprocessing.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.eventprocessing.EventProcessingConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/26/2015.
 */

public class PrepareCrawledPageDocument extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String eventType = tridentTuple.getString(0);
        String uri = tridentTuple.getString(1);
        String description = tridentTuple.getString(2);
        String task = tridentTuple.getString(3);
        String content_html = tridentTuple.getString(4);
        String title = tridentTuple.getString(5);


        //Build JSON String
        JSONObject sourceJsonObj = new JSONObject();

        sourceJsonObj.put("event_type", eventType);
        sourceJsonObj.put("uri", uri);
        sourceJsonObj.put("title", title);
        sourceJsonObj.put("description", (EventProcessingConfig.EVENT_TYPE_URL.equals( eventType) ? content_html : description ));

//        System.out.println("----- PrepareCrawledPageDocument: source = "+sourceJsonObj.toString());

        tridentCollector.emit(new Values(EventProcessingConfig.ELASTICSEARCH_INDEX_NAME, task, "", sourceJsonObj.toString() ));
    }
}
