package storm.eventprocessing.function;

import backtype.storm.tuple.Values;
import storm.eventprocessing.common.Readability;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.net.URL;

/**
 * Created by Sunil Kalmadka on 4/26/15.
 */
public class SplitEventString  extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String str = tridentTuple.getString(0);

        String eventType;
        String url;
        String description;
        String task;
        String crawl_depth;

        try {
            String[] strSplit = str.split(" ");
            eventType = strSplit[0];
            url = strSplit[1];
            description = strSplit[2];
            task = strSplit[3];
            crawl_depth = (strSplit[4] == null || strSplit[4] == "") ? "3" : strSplit[4];
        } catch(Exception e){
            System.err.println("----- Event Rejected: InValid Event Format: " + str);
            return;
        }
        tridentCollector.emit(new Values(eventType, url, description, task, crawl_depth));
    }
}
