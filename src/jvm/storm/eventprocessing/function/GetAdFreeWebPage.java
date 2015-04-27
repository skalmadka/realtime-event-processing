package storm.eventprocessing.function;


import backtype.storm.tuple.Values;
import storm.eventprocessing.EventProcessingConfig;
import storm.eventprocessing.common.Readability;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import java.io.IOException;
import java.net.URL;

/**
 * Created by Sunil Kalmadka on 4/26/2015.
 */

public class GetAdFreeWebPage  extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String eventType =  tridentTuple.getString(0);
        String url = tridentTuple.getString(1);

        if(!EventProcessingConfig.EVENT_TYPE_URL.equals(eventType))
            tridentCollector.emit(new Values("", "", ""));

        Readability readability = null;
        Integer timeoutMillis = 4000;

        try {
            readability = new Readability(new URL(url), timeoutMillis);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        readability.init();

        String webPageString = readability.content; //readability.outerHtml();
        String webPageTitle = readability.title;
        String hrefString = readability.hrefString.toString();

        System.out.println("GetAdFreeWebPage: hrefString: \""+ hrefString+"\"");
        tridentCollector.emit(new Values(webPageString, webPageTitle, hrefString));
    }
}
