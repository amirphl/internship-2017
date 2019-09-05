package crawler;

import com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.Pair;
import utils.Statistics;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static crawler.Crawler.urlQueue;
import static storage.HBase.setOfInsertedLinksAsRow;

public class Fetcher implements Runnable {

    private int threadNum;
    private Logger logger = LoggerFactory.getLogger(Crawler.class);

    Fetcher(int threadNum) {
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        logger.info("fetcher {} Started.", threadNum);
        String link;
        String domain = null;
        Document document;
        Pair<String, Document> fetchedData;
        while (true) {
            try {
                link = takeUrl();
            } catch (InterruptedException e) {
//                logger.error("Fetcher {} couldn't take link from queue.\n{}", threadNum, Prints.getPrintStackTrace(e));
                logger.error("fetcher {} couldn't take link from queue\n", threadNum);
                continue;
            }
            if (link == null || link.isEmpty()) {
                logger.warn("fetcher {} got null or empty link from queue\n", threadNum);
                continue;
            }
            try {
                domain = getDomainIfLruAllowed(link);
            } catch (Exception e) {
//                logger.error("fetcher {} couldn't extract domain of {}\n{}", threadNum, link, Prints.getPrintStackTrace(e));
                logger.error("fetcher {} couldn't extract domain of {}\n", threadNum, link);
            }
            if (domain == null) {
//                ProducerApp.send(Constants.URL_TOPIC, link);
                try {
                    urlQueue.put(link);
                } catch (InterruptedException ignored) {

                }
                continue;
            }

            if (checkWithHBase(link))
                continue;
//            logger.error("Fetcher {} couldn't check {} with HBase.\n{}", threadNum, link, Prints.getPrintStackTrace(e));
//            ProducerApp.send(Constants.URL_TOPIC, link);


            try {
                document = fetch(link);
            } catch (IOException | IllegalArgumentException | IllegalStateException e) {
                Statistics.getInstance().addFetcherFailedToFetch(threadNum);
//                logger.error("fetcher {} timeout reached or connection refused. couldn't connect to {}\n{}", threadNum, link, Prints.getPrintStackTrace(e));
                logger.error("fetcher {} timeout reached or connection refused. couldn't connect to {}\n", threadNum, link);
                continue;
            }

            fetchedData = new Pair<>();
            fetchedData.setKeyVal(link, document);
            try {
                putFetchedData(fetchedData);
            } catch (InterruptedException e) {
//                logger.error("fetcher {} while putting fetched data in queue:\n{}", threadNum, Prints.getPrintStackTrace(e));
                logger.error("fetcher {} while putting fetched data in queue\n", threadNum);
            }
        }
    }

    private String takeUrl() throws InterruptedException {
        long time = System.currentTimeMillis();
        String link = Crawler.urlQueue.take();
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherTakeUrlTime(time, threadNum);
        logger.info("fetcher {} took {} from Q in time {}ms", threadNum, link, time);
        return link;
    }

    private String getDomainIfLruAllowed(String link) throws IllegalArgumentException, IllegalStateException, MalformedURLException {
        long time = System.currentTimeMillis();
        URL url = new URL(link);
        String domain = url.getHost();
        domain = InternetDomainName.from(domain).topPrivateDomain().toString();

        if (domain == null || domain.isEmpty()) {
            throw new IllegalArgumentException("domain is null or empty");
        }
        boolean exist = LruCache.getInstance().exist(domain);
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherLruCheckTime(time, threadNum);
        if (exist) {
            logger.info("fetcher {} domain {} is not allowed. Back to Queue", threadNum, domain);
            Statistics.getInstance().addFetcherFailedLru(threadNum);
            return null;
        } else {
            logger.info("fetcher {} domain {} is allowed.", threadNum, domain);
            return domain;
        }
    }

    private boolean checkWithHBase(String link) {
        long time = System.currentTimeMillis();
        boolean result;
        synchronized (Parser.class) {
            result = setOfInsertedLinksAsRow.contains(link.hashCode());
        }
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherHBaseCheckTime(time, threadNum);
        return result;
    }

    private Document fetch(String link) throws IOException, IllegalStateException, IllegalArgumentException {
        logger.info("fetcher {} connecting to {} ... ", threadNum, link);
        Long connectTime = System.currentTimeMillis();
        Document doc = Jsoup.connect(link)
                .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                .ignoreHttpErrors(true).timeout(Constants.FETCH_TIMEOUT).get();
        connectTime = System.currentTimeMillis() - connectTime;
        Statistics.getInstance().addFetcherFetchTime(connectTime, threadNum);
        logger.info("fetcher {} connected in {}ms to {}", threadNum, connectTime, link);
        return doc;
    }

    private void putFetchedData(Pair<String, Document> forParseData) throws InterruptedException {
        long time = System.currentTimeMillis();
        Crawler.fetchedData.put(forParseData);
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherPutFetchedDataTime(time, threadNum);
    }
}
