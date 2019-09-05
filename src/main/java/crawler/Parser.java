package crawler;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.HBase;
import utils.Pair;
import utils.Statistics;

import java.io.IOException;
import java.util.*;

import static crawler.Crawler.urlQueue;

public class Parser implements Runnable {

    private int threadNum;
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private HBase hbase = new HBase();

    Parser(int threadNum) throws IOException {
        this.threadNum = threadNum;
    }


    //todo fix language detection
    @Override
    public void run() {
        logger.info("parser {} started.\n", threadNum);

//        LanguageDetector languageDetector = null;
//        try {
//            List<LanguageProfile> lp = new LanguageProfileReader().readAllBuiltIn();
//            languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
//                    .withProfiles(lp)
//                    .build();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        String link;
        String title;
        String content;
        Document document;
        Pair<String, String>[] linkAnchors;
        Pair<String, Document> fetchedData;
        StringBuilder contentBuilder;
        long time;

        while (true) {
            time = System.currentTimeMillis();
            try {
                fetchedData = takeFetchedData();
            } catch (InterruptedException e) {
//                logger.error("parser {} while taking fetched data from queue:\n{}", threadNum, Prints.getPrintStackTrace(e));
                logger.error("parser {} while taking fetched data from queue\n", threadNum);
                continue;
            }

            link = fetchedData.getKey();
            document = fetchedData.getValue();
            title = document.title();
            contentBuilder = new StringBuilder();
            for (Element element : document.select("p")) {
                contentBuilder.append(element.text()).append("\n");
            }
            content = contentBuilder.toString();

//            long t = System.currentTimeMillis();
//            TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
//            TextObject textObject = textObjectFactory.forText(content);
//            Optional<LdLocale> lang = languageDetector.detect(textObject);

            //todo fix this
            linkAnchors = extractLinkAnchors(document).toArray(new Pair[0]);
            Statistics.getInstance().addParserParseTime(System.currentTimeMillis() - time, threadNum);
            try {
                putToElastic(link, title, content);
            } catch (IOException e) {
//                logger.error("parser {} while putting in ElasticSearch:\n{}", threadNum, Prints.getPrintStackTrace(e));
                logger.error("parser {} while putting in ElasticSearch\n", threadNum);
            }

            try {
                putAnchorsToHBase(link, linkAnchors);
            } catch (IOException e) {
//                logger.error("parser {} while adding in HBase:\n{}", threadNum, Prints.getPrintStackTrace(e));
                logger.error("parser {} while adding in HBase\n", threadNum);
            }

            checkWithHBase(linkAnchors);

            try {
                putToKafka(linkAnchors);
            } catch (InterruptedException e) {
                logger.error("parser {} while adding in urlQ\n", threadNum);
            }
        }
    }

    private Pair<String, Document> takeFetchedData() throws InterruptedException {
        long time = System.currentTimeMillis();
        Pair<String, Document> fetchedData = Crawler.fetchedData.take();
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserTakeFetchedData(time, threadNum);
        return fetchedData;
    }

    private Set<Pair<String, String>> extractLinkAnchors(Document document) {
        Set<Pair<String, String>> linksAnchors = new HashSet<>();
        for (Element element : document.select("a[href]")) {
            String extractedLink = element.attr("abs:href");
            String anchor = element.text();
            if (extractedLink == null || extractedLink.isEmpty()) {
                continue;
            }
            Pair<String, String> linkAnchor = new Pair<>();
            linkAnchor.setKeyVal(extractedLink, anchor);
            linksAnchors.add(linkAnchor);
        }
        return linksAnchors;
    }

    private void putToElastic(String link, String title, String content) throws IOException {
        long time = System.currentTimeMillis();
        Crawler.elasticEngine.indexData(link, content, title);
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserElasticPutTime(time, threadNum);
        logger.info("parser {} data added to ElasticSearch in {}ms for {}\n", threadNum, time, link);
    }

    private void putAnchorsToHBase(String link, Pair<String, String>[] linkAnchors) throws IOException {
        long time = System.currentTimeMillis();
        hbase.insertLinks(link, linkAnchors);
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserHBasePutTime(time, threadNum);
        logger.info("Parser {} data added to HBase in {}ms for {}\n", threadNum, time, link);
    }

    private void checkWithHBase(Pair<String, String>[] linkAnchors) {
        long time = System.currentTimeMillis();
        hbase.checkAndReplace(linkAnchors);
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserHBaseCheckTime(time, threadNum);
    }

    private void putToKafka(Pair<String, String>[] linkAnchors) throws InterruptedException {
        long time = System.currentTimeMillis();
        for (Pair<String, String> linkAnchor : linkAnchors) {
            if (linkAnchor != null) {
                //                ProducerApp.send(Constants.URL_TOPIC, linkAnchor.getKey());
                urlQueue.put(linkAnchor.getKey());
            }
        }
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserKafkaPutTime(time, threadNum);
    }

}