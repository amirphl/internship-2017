package crawler;

import index.Elastic;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.Pair;
import utils.Statistics;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

    static Elastic elasticEngine;
    public static ArrayBlockingQueue<String> urlQueue;
    public static ArrayBlockingQueue<Pair<String, Document>> fetchedData;
    private static Logger logger;

    //todo fix data loss after shutdown because of urlQueue and fetchedData and setOfInsertedLinksAsRow queues.
    static {
        urlQueue = new ArrayBlockingQueue<>(Constants.URL_QUEUE_SIZE);
        fetchedData = new ArrayBlockingQueue<>(Constants.FETCHED_DATA_QUEUE_SIZE);
        logger = LoggerFactory.getLogger(Crawler.class);
        elasticEngine = Elastic.getInstance();
    }

    //todo fix kafka
    //todo filter protocols: FTP , ...
    public static void main(String args[]) throws IOException, InterruptedException {
//        DetectorFactory.loadProfile("/home/nimbo_search/amirphl/profiles");
        logger.info("Seed added.");
//        urlQueue.put("https://en.wikipedia.org/wiki/Main_Page");
//        urlQueue.put("https://us.yahoo.com/");
//        urlQueue.put("https://www.nytimes.com/");
//        urlQueue.put("https://www.msn.com/en-us/news");
//        urlQueue.put("http://www.telegraph.co.uk/news/");
//        urlQueue.put("http://www.alexa.com");
//        urlQueue.put("http://www.apache.org");
//        urlQueue.put("https://en.wikipedia.org/wiki/Main_Page/World_war_II");
//        urlQueue.put("http://www.news.google.com");
//        urlQueue.put("https://www.geeksforgeeks.org");
        urlQueue.put("https://mvnrepository.com/");
//        ProducerApp.send(Constants.URL_TOPIC, "https://en.wikipedia.org/wiki/Main_Page");
//        ProducerApp.send(Constants.URL_TOPIC, "https://us.yahoo.com/");
//        ProducerApp.send(Constants.URL_TOPIC, "https://www.nytimes.com/");
//        ProducerApp.send(Constants.URL_TOPIC, "https://www.msn.com/en-us/news");
//        ProducerApp.send(Constants.URL_TOPIC, "http://www.telegraph.co.uk/news/");
//        ProducerApp.send(Constants.URL_TOPIC, "http://www.alexa.com");
//        ProducerApp.send(Constants.URL_TOPIC, "http://www.apache.org");
//        ProducerApp.send(Constants.URL_TOPIC, "https://en.wikipedia.org/wiki/Main_Page/World_war_II");
//        ProducerApp.send(Constants.URL_TOPIC, "http://www.news.google.com");

        Statistics.getInstance().setThreadsNums(Constants.FETCHER_NUMBER, Constants.PARSER_NUMBER);
        Thread stat = new Thread(Statistics.getInstance());
        stat.start();

        for (int i = 0; i < Constants.PARSER_NUMBER; i++) {
            new Thread(new Parser(i)).start();
        }
        for (int i = 0; i < Constants.FETCHER_NUMBER; i++) {
            new Thread(new Fetcher(i)).start();
        }

//        ConsumerApp consumerApp = new ConsumerApp();
//        consumerApp.start();
    }
}