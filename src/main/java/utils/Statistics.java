package utils;

import crawler.Crawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Statistics implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Logger statLog = LoggerFactory.getLogger("statLogger");
    private Logger periodLogger = LoggerFactory.getLogger("periodLogger");
    private ArrayList<Map<String, Long>> threadsTimes;
    private ConcurrentHashMap<String, Long> newTotal;
    private ConcurrentHashMap<String, Long> oldTotal;
    private ConcurrentHashMap<String, Long> realtimeStatistic;

    private int fetcherThreadNum;
    private int parserThreadNum;


    private final String PARSER_TAKE_FETCHED_DATA_TIME = "parserTakeFetchedDataTime";
    private final String PARSER_TAKE_FETCHED_DATA_NUM = "parserTakeFetchedDataNum";
    private final String PARSER_PARSE_TIME = "parseTime";
    private final String PARSER_PARSE_NUM = "parseNum";
    private final String PARSER_ELASTIC_PUT_TIME = "parserElasticPutTime";
    private final String PARSER_ELASTIC_PUT_NUM = "parserElasticPutNum";
    private final String PARSER_HBASE_PUT_TIME = "parserHBasePutTime";
    private final String PARSER_HBASE_PUT_NUM = "parserHBasePutNum";
    private final String PARSER_HBASE_CHECK_TIME = "parserHBaseCheckTime";
    private final String PARSER_HBASE_CHECK_NUM = "parserHBaseCheckNum";
    private final String PARSER_KAFKA_PUT_TIME = "parserKafkaPutTime";
    private final String PARSER_KAFKA_PUT_NUM = "parserKafkaPutNum";

    private final String FETCHER_TAKE_URL_TIME = "fetcherTakeUrlTime";
    private final String FETCHER_TAKE_URL_NUM = "fetcherTakeUrlNum";
    private final String FETCHER_LRU_CHECK_TIME = "fetcherLruCheckTime";
    private final String FETCHER_LRU_CHECK_NUM = "fetcherLruCheckNum";
    private final String FETCHER_FAILED_LRU_NUM = "fetcherFailedLruNum";
    private final String FETCHER_HBASE_CHECK_TIME = "fetcherHBaseCheckTime";
    private final String FETCHER_HBASE_CHECK_NUM = "fetcherHBaseCheckNum";
    private final String FETCHER_FETCH_TIME = "fetcherFetchTime";
    private final String FETCHER_FETCH_NUM = "fetcherFetchNum";
    private final String FETCHER_FAILED_TO_FETCH_NUM = "fetcherFailedToFetchNum";
    private final String FETCHER_PUT_FETCHED_DATA_TIME = "fetcherPutFetchedDataTime";
    private final String FETCHER_PUT_FETCHED_DATA_NUM = "fetcherPutFetchedDataNum";

    private final String URL_PUT_Q_TIME = "urlPutQTime";
    private final String URL_PUT_Q_NUM = "urlPutQNum";
    private final String DOC_TAKE_TIME = "docTakeTime";
    private final String DOC_TAKE_NUM = "docTakeNum";
    private final String DOC_PUT_TIME = "docPutTime";
    private final String DOC_PUT_NUM = "docPutNum";
    private final String TIMESTAMP = "timestamp";

    private final String RATE = "rate";

    private static Statistics myStat = null;

    private final int REFRESH_TIME = Constants.STATISTIC_REFRESH_TIME / 1000;


    public synchronized static Statistics getInstance() {
        if (myStat == null) {
            myStat = new Statistics();
        }
        return myStat;
    }

    private void logStats() {

        newTotal = new ConcurrentHashMap<>();
        long first;
        long second;

        for (int i = 0; i < parserThreadNum; i++) {
            Map<String, Long> thread = threadsTimes.get(i);

            first = thread.get(PARSER_TAKE_FETCHED_DATA_TIME);
            second = thread.get(PARSER_TAKE_FETCHED_DATA_NUM);
            statLog.info("Parser{} parse time is {} and parse num {}", i, first, second);
            statLog.info("Parser{} average parse time is : {}", i, first / second);
            addToTotal(PARSER_TAKE_FETCHED_DATA_TIME, first);
            addToTotal(PARSER_TAKE_FETCHED_DATA_NUM, second);

            first = thread.get(PARSER_PARSE_TIME);
            second = thread.get(PARSER_PARSE_NUM);
            statLog.info("Parser{} parse time is {} and parse num {}", i, first, second);
            statLog.info("Parser{} average parse time is : {}", i, first / second);
            addToTotal(PARSER_PARSE_TIME, first);
            addToTotal(PARSER_PARSE_NUM, second);

            first = thread.get(PARSER_ELASTIC_PUT_TIME);
            second = thread.get(PARSER_ELASTIC_PUT_NUM);
            statLog.info("Parser{} elastic put time is {} and num is {}", i, first, second);
            statLog.info("Parser{} average elastic put time is : {}\n", i, first / second);
            addToTotal(PARSER_ELASTIC_PUT_TIME, first);
            addToTotal(PARSER_ELASTIC_PUT_NUM, second);

            first = thread.get(PARSER_HBASE_PUT_TIME);
            second = thread.get(PARSER_HBASE_PUT_NUM);
            statLog.info("Parser{} HBase put time is {} and num is {}", i, first, second);
            statLog.info("Parser{} average Hbase put time is : {}", i, first / second);
            addToTotal(PARSER_HBASE_PUT_TIME, first);
            addToTotal(PARSER_HBASE_PUT_NUM, second);

            first = thread.get(PARSER_HBASE_CHECK_TIME);
            second = thread.get(PARSER_HBASE_CHECK_NUM);
            statLog.info("Parser{} HBase exist time is {} and num is {}", i, first, second);
            statLog.info("Parser{} average Hbase exist time is : {}", i, first / second);
            addToTotal(PARSER_HBASE_CHECK_TIME, first);
            addToTotal(PARSER_HBASE_CHECK_NUM, second);

            first = thread.get(PARSER_KAFKA_PUT_TIME);
            second = thread.get(PARSER_KAFKA_PUT_NUM);
            statLog.info("Parser{} url put time is {} and num is {}", i, first, second);
            statLog.info("Parser{} average url put time is : {}", i, first / second);
            addToTotal(PARSER_KAFKA_PUT_TIME, first);
            addToTotal(PARSER_KAFKA_PUT_NUM, second);

        }
        for (int i = 0; i < fetcherThreadNum; i++) {
            Map<String, Long> thread = threadsTimes.get(i);

            first = thread.get(FETCHER_TAKE_URL_TIME);
            second = thread.get(FETCHER_TAKE_URL_NUM);
            statLog.info("Fetcher{} url take time is {}, take num is {}", i, first, second);
            statLog.info("Fetcher{} average url take time is : {}", i, first / second);
            addToTotal(FETCHER_TAKE_URL_TIME, first);
            addToTotal(FETCHER_TAKE_URL_NUM, second);

            first = thread.get(FETCHER_LRU_CHECK_TIME);
            second = thread.get(FETCHER_LRU_CHECK_NUM);
            statLog.info("Fetcher{} url take time is {}, take num is {}", i, first, second);
            statLog.info("Fetcher{} average url take time is : {}", i, first / second);
            addToTotal(FETCHER_LRU_CHECK_TIME, first);
            addToTotal(FETCHER_LRU_CHECK_NUM, second);

            first = thread.get(FETCHER_FAILED_LRU_NUM);
            statLog.info("Fetcher{} url take time is {}, take num is {}", i, first, second);
            addToTotal(FETCHER_FAILED_LRU_NUM, first);

            first = thread.get(FETCHER_HBASE_CHECK_TIME);
            second = thread.get(FETCHER_HBASE_CHECK_NUM);
            statLog.info("Fetcher{} url take time is {}, take num is {}", i, first, second);
            statLog.info("Fetcher{} average url take time is : {}", i, first / second);
            addToTotal(FETCHER_HBASE_CHECK_TIME, first);
            addToTotal(FETCHER_HBASE_CHECK_NUM, second);

            first = thread.get(FETCHER_FETCH_TIME);
            second = thread.get(FETCHER_FETCH_NUM);
            statLog.info("Fetcher{} fetch time is {}, fetch num is {}", i, first, second);
            statLog.info("Fetcher{} average fetch time is : {}", i, first / second);
            addToTotal(FETCHER_FETCH_TIME, first);
            addToTotal(FETCHER_FETCH_NUM, second);

            first = thread.get(FETCHER_FAILED_TO_FETCH_NUM);
            statLog.info("Fetcher{} had {} failed connection\n", i, first);
            addToTotal(FETCHER_FAILED_TO_FETCH_NUM, first);

            first = thread.get(FETCHER_PUT_FETCHED_DATA_TIME);
            second = thread.get(FETCHER_PUT_FETCHED_DATA_NUM);
            statLog.info("Fetcher{} fetch time is {}, fetch num is {}", i, first, second);
            statLog.info("Fetcher{} average fetch time is : {}", i, first / second);
            addToTotal(FETCHER_PUT_FETCHED_DATA_TIME, first);
            addToTotal(FETCHER_PUT_FETCHED_DATA_NUM, second);
        }

//        first = newTotal.get(PARSER_PARSE_NUM);
//        second =  newTotal.get(PARSER_PARSE_TIME);
//        avgStatLogger.info("{} links parsed in {}ms", first,second);
//        avgStatLogger.info("average links/sec is {}", second/first);


//        first = newTotal.get(URL_PUT_Q_NUM);
//        second = newTotal.get(URL_PUT_Q_TIME);
//        avgStatLogger.info("{} links added in Kafka in {}ms", first,second);
//        avgStatLogger.info("average links/sec added in Kafka is {}", second/first);
//
//        first = newTotal.get(FETCHER_HBASE_CHECK_NUM);
//        second = newTotal.get(FETCHER_HBASE_CHECK_TIME);
//        avgStatLogger.info("{} links checked with HBase in {}ms", first,second);
//        avgStatLogger.info("average links/sec checked with HBase is {}", second/first);
//
//        first = newTotal.get(PARSER_HBASE_PUT_NUM);
//        second = newTotal.get(PARSER_HBASE_PUT_TIME);
//        avgStatLogger.info("{} links added to HBase in {}ms", first,second);
//        avgStatLogger.info("average links/sec added to HBase is {}", second/first);
//
//        first = newTotal.get(PARSER_ELASTIC_PUT_NUM);
//        second = newTotal.get(PARSER_ELASTIC_PUT_TIME);
//        avgStatLogger.info("{} links added to Elastic in {}ms", first,second);
//        avgStatLogger.info("average links/sec added to Elastic is {}", second/first);
//
//        first = newTotal.get(FETCHER_FETCH_NUM);
//        second = newTotal.get(FETCHER_FETCH_TIME);
//        avgStatLogger.info("{} links fetched in {}ms", first,second);
//        avgStatLogger.info("average fetch time is {}", second/first);
//
//        first = newTotal.get(FETCHER_TAKE_URL_NUM);
//        second = newTotal.get(FETCHER_TAKE_URL_TIME);
//        avgStatLogger.info("{} links taked from buffer in {}ms", first,second);
//        avgStatLogger.info("average buffer time is {}", second/first);
//
//        first = newTotal.get(FETCHER_FAILED_TO_FETCH_NUM);
//        avgStatLogger.info("failed to connect to {} links\n", first);

        if (oldTotal != null) {
            realtimeStatistic = new ConcurrentHashMap<>();
            realtimeStatistic.put(TIMESTAMP, System.currentTimeMillis());

            first = newTotal.get(PARSER_TAKE_FETCHED_DATA_NUM) - oldTotal.get(PARSER_TAKE_FETCHED_DATA_NUM);
            second = newTotal.get(PARSER_TAKE_FETCHED_DATA_TIME) - oldTotal.get(PARSER_TAKE_FETCHED_DATA_TIME);
            realtimeStatistic.put(PARSER_TAKE_FETCHED_DATA_NUM, first);
            realtimeStatistic.put(PARSER_TAKE_FETCHED_DATA_TIME, second);
            periodLogger.info("Parser: {} fetchedLink taken from queue in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);


            first = newTotal.get(PARSER_PARSE_NUM) - oldTotal.get(PARSER_PARSE_NUM);
            second = newTotal.get(PARSER_PARSE_TIME) - oldTotal.get(PARSER_PARSE_TIME);
            realtimeStatistic.put(PARSER_PARSE_NUM, first);
            realtimeStatistic.put(PARSER_PARSE_TIME, second);
            periodLogger.info("Parser: {} fetchedLink parsed in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);


            first = newTotal.get(PARSER_ELASTIC_PUT_NUM) - oldTotal.get(PARSER_ELASTIC_PUT_NUM);
            second = newTotal.get(PARSER_ELASTIC_PUT_TIME) - oldTotal.get(PARSER_ELASTIC_PUT_TIME);
            realtimeStatistic.put(PARSER_ELASTIC_PUT_NUM, first);
            realtimeStatistic.put(PARSER_ELASTIC_PUT_TIME, second);
            periodLogger.info("Parser: {} links added in Elastic in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            first = newTotal.get(PARSER_HBASE_PUT_NUM) - oldTotal.get(PARSER_HBASE_PUT_NUM);
            second = newTotal.get(PARSER_HBASE_PUT_TIME) - oldTotal.get(PARSER_HBASE_PUT_TIME);
            realtimeStatistic.put(PARSER_HBASE_PUT_NUM, first);
            realtimeStatistic.put(PARSER_HBASE_PUT_TIME, second);
            periodLogger.info("Parser: {} links putted in HBase in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            Long rate = first / REFRESH_TIME;

            first = newTotal.get(PARSER_HBASE_CHECK_NUM) - oldTotal.get(PARSER_HBASE_CHECK_NUM);
            second = newTotal.get(PARSER_HBASE_CHECK_TIME) - oldTotal.get(PARSER_HBASE_CHECK_TIME);
            realtimeStatistic.put(PARSER_HBASE_CHECK_NUM, first);
            realtimeStatistic.put(PARSER_HBASE_CHECK_TIME, second);
            periodLogger.info("Parser: {} links checked with HBase in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            first = newTotal.get(PARSER_KAFKA_PUT_NUM) - oldTotal.get(PARSER_KAFKA_PUT_NUM);
            second = newTotal.get(PARSER_KAFKA_PUT_TIME) - oldTotal.get(PARSER_KAFKA_PUT_TIME);
            realtimeStatistic.put(PARSER_KAFKA_PUT_NUM, first);
            realtimeStatistic.put(PARSER_KAFKA_PUT_TIME, second);
            periodLogger.info("Parser: {} links putted in kafka in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);


            first = newTotal.get(FETCHER_TAKE_URL_NUM) - oldTotal.get(FETCHER_TAKE_URL_NUM);
            second = newTotal.get(FETCHER_TAKE_URL_TIME) - oldTotal.get(FETCHER_TAKE_URL_TIME);
            realtimeStatistic.put(FETCHER_TAKE_URL_NUM, first);
            realtimeStatistic.put(FETCHER_TAKE_URL_TIME, second);
            periodLogger.info("Fetcher: {} links taken from queue in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            first = newTotal.get(FETCHER_LRU_CHECK_NUM) - oldTotal.get(FETCHER_LRU_CHECK_NUM);
            second = newTotal.get(FETCHER_LRU_CHECK_TIME) - oldTotal.get(FETCHER_LRU_CHECK_TIME);
            realtimeStatistic.put(FETCHER_LRU_CHECK_NUM, first);
            realtimeStatistic.put(FETCHER_LRU_CHECK_TIME, second);
            periodLogger.info("Fetcher: {} links checked with lru in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            first = newTotal.get(FETCHER_FAILED_LRU_NUM) - oldTotal.get(FETCHER_FAILED_LRU_NUM);
            realtimeStatistic.put(FETCHER_FAILED_LRU_NUM, first);
            periodLogger.info("Fetcher: {} links rejected by lru", first);

            first = newTotal.get(FETCHER_HBASE_CHECK_NUM) - oldTotal.get(FETCHER_HBASE_CHECK_NUM);
            second = newTotal.get(FETCHER_HBASE_CHECK_TIME) - oldTotal.get(FETCHER_HBASE_CHECK_TIME);
            realtimeStatistic.put(FETCHER_HBASE_CHECK_NUM, first);
            realtimeStatistic.put(FETCHER_HBASE_CHECK_TIME, second);
            periodLogger.info("Fetcher: {} links checked with HBase in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            first = newTotal.get(FETCHER_FETCH_NUM) - oldTotal.get(FETCHER_FETCH_NUM);
            second = newTotal.get(FETCHER_FETCH_TIME) - oldTotal.get(FETCHER_FETCH_TIME);
            realtimeStatistic.put(FETCHER_FETCH_NUM, first);
            realtimeStatistic.put(FETCHER_FETCH_TIME, second);
            periodLogger.info("Fetcher: {} links fetched in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);


            first = newTotal.get(FETCHER_FAILED_TO_FETCH_NUM) - oldTotal.get(FETCHER_FAILED_TO_FETCH_NUM);
            realtimeStatistic.put(FETCHER_FAILED_TO_FETCH_NUM, first);
            periodLogger.info("Fetcher: {} links failed to fetch", first);

            first = newTotal.get(FETCHER_PUT_FETCHED_DATA_NUM) - oldTotal.get(FETCHER_PUT_FETCHED_DATA_NUM);
            second = newTotal.get(FETCHER_PUT_FETCHED_DATA_TIME) - oldTotal.get(FETCHER_PUT_FETCHED_DATA_TIME);
            realtimeStatistic.put(FETCHER_PUT_FETCHED_DATA_NUM, first);
            realtimeStatistic.put(FETCHER_PUT_FETCHED_DATA_TIME, second);
            periodLogger.info("Fetcher: {} fetchedLinks putted in queue in {}ms", first, second);
            if (first != 0)
                periodLogger.info("\taverage time per link = {} ms/link", second / first);

            periodLogger.info("links in wait: {}", Crawler.urlQueue.size());
            periodLogger.info("documents in wait: {}", Crawler.fetchedData.size());
            periodLogger.info("\t Crawler Rate is {} links/sec\n", rate);

            realtimeStatistic.put(RATE, rate);

//            sendStatistic();
        }
        oldTotal = newTotal;
    }

    private void addToTotal(String key, Long value) {
        if (!newTotal.containsKey(key)) {
            newTotal.put(key, value);
        } else {
            Long oldVal = newTotal.get(key);
            Long newVal = oldVal + value;
            newTotal.put(key, newVal);
        }
    }

    public void setThreadsNums(int fetcherThreadNum, int parserThreadNum) {
        this.fetcherThreadNum = fetcherThreadNum;
        this.parserThreadNum = parserThreadNum;
        threadsTimes = new ArrayList<>();
        int max = Math.max(fetcherThreadNum, parserThreadNum);
        for (int i = 0; i < max; i++) {
            ConcurrentHashMap<String, Long> tmp = new ConcurrentHashMap<>();

            tmp.put(PARSER_TAKE_FETCHED_DATA_TIME, 1L);
            tmp.put(PARSER_TAKE_FETCHED_DATA_NUM, 1L);
            tmp.put(PARSER_PARSE_TIME, 1L);
            tmp.put(PARSER_PARSE_NUM, 1L);
            tmp.put(PARSER_ELASTIC_PUT_TIME, 1L);
            tmp.put(PARSER_ELASTIC_PUT_NUM, 1L);
            tmp.put(PARSER_HBASE_PUT_TIME, 1L);
            tmp.put(PARSER_HBASE_PUT_NUM, 1L);
            tmp.put(PARSER_HBASE_CHECK_TIME, 1L);
            tmp.put(PARSER_HBASE_CHECK_NUM, 1L);
            tmp.put(PARSER_KAFKA_PUT_TIME, 1L);
            tmp.put(PARSER_KAFKA_PUT_NUM, 1L);

            tmp.put(FETCHER_TAKE_URL_TIME, 1L);
            tmp.put(FETCHER_TAKE_URL_NUM, 1L);
            tmp.put(FETCHER_LRU_CHECK_TIME, 1L);
            tmp.put(FETCHER_LRU_CHECK_NUM, 1L);
            tmp.put(FETCHER_FAILED_LRU_NUM, 1L);
            tmp.put(FETCHER_HBASE_CHECK_TIME, 1L);
            tmp.put(FETCHER_HBASE_CHECK_NUM, 1L);
            tmp.put(FETCHER_FETCH_TIME, 1L);
            tmp.put(FETCHER_FETCH_NUM, 1L);
            tmp.put(FETCHER_FAILED_TO_FETCH_NUM, 1L);
            tmp.put(FETCHER_PUT_FETCHED_DATA_TIME, 1L);
            tmp.put(FETCHER_PUT_FETCHED_DATA_NUM, 1L);

            tmp.put(URL_PUT_Q_TIME, 1L);
            tmp.put(URL_PUT_Q_NUM, 1L);
            tmp.put(DOC_TAKE_TIME, 1L);
            tmp.put(DOC_TAKE_NUM, 1L);
            tmp.put(DOC_PUT_TIME, 1L);
            tmp.put(DOC_PUT_NUM, 1L);
            threadsTimes.add(tmp);
        }
        System.out.println("test");
    }

    public void addParserTakeFetchedData(Long parserTakeFetchedData, int threadNum) {
        addTime(parserTakeFetchedData, threadNum, PARSER_TAKE_FETCHED_DATA_TIME, PARSER_TAKE_FETCHED_DATA_NUM);
    }

    public void addParserParseTime(Long parseTime, int threadNum) {
        addTime(parseTime, threadNum, PARSER_PARSE_TIME, PARSER_PARSE_NUM);
    }

    public void addParserElasticPutTime(Long elasticPutTime, int threadNum) {
        addTime(elasticPutTime, threadNum, PARSER_ELASTIC_PUT_TIME, PARSER_ELASTIC_PUT_NUM);
    }

    public void addParserHBasePutTime(Long hBasePutTime, int threadNum) {
        addTime(hBasePutTime, threadNum, PARSER_HBASE_PUT_TIME, PARSER_HBASE_PUT_NUM);
    }

    public void addParserHBaseCheckTime(Long hBaseCheckTime, int threadNum) {
        addTime(hBaseCheckTime, threadNum, PARSER_HBASE_CHECK_TIME, PARSER_HBASE_CHECK_NUM);
    }

    public void addParserKafkaPutTime(Long kafkaPutTime, int threadNum) {
        addTime(kafkaPutTime, threadNum, PARSER_KAFKA_PUT_TIME, PARSER_KAFKA_PUT_NUM);
    }

    public void addFetcherTakeUrlTime(Long takeUrlTime, int threadNum) {
        addTime(takeUrlTime, threadNum, FETCHER_TAKE_URL_TIME, FETCHER_TAKE_URL_NUM);
    }

    public void addFetcherLruCheckTime(long lruCheckTime, int threadNum) {
        addTime(lruCheckTime, threadNum, FETCHER_LRU_CHECK_TIME, FETCHER_LRU_CHECK_NUM);
    }

    public void addFetcherFailedLru(int threadNum) {
        addNum(threadNum, FETCHER_FAILED_LRU_NUM);
    }

    public void addFetcherHBaseCheckTime(Long hBaseCheckTime, int threadNum) {
        addTime(hBaseCheckTime, threadNum, FETCHER_HBASE_CHECK_TIME, FETCHER_HBASE_CHECK_NUM);
    }

    public void addFetcherFetchTime(Long fetchTime, int threadNum) {
        addTime(fetchTime, threadNum, FETCHER_FETCH_TIME, FETCHER_FETCH_NUM);
    }

    public void addFetcherFailedToFetch(int threadNum) {
        addNum(threadNum, FETCHER_FAILED_TO_FETCH_NUM);
    }

    public void addFetcherPutFetchedDataTime(Long putTime, int threadNum) {
        addTime(putTime, threadNum, FETCHER_PUT_FETCHED_DATA_TIME, FETCHER_PUT_FETCHED_DATA_NUM);
    }

    private void addTime(Long time, int threadNum, String timeName, String numName) {
        Long oldTime = threadsTimes.get(threadNum).get(timeName);
        Long newTime = oldTime + time;
        threadsTimes.get(threadNum).put(timeName, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(numName);
        Long newNum = ++oldNum;
        threadsTimes.get(threadNum).put(numName, newNum);
    }

    private void addNum(int threadNum, String numName) {
        Long newNum = threadsTimes.get(threadNum).get(numName) + 1;
        threadsTimes.get(threadNum).put(numName, newNum);
    }

    @Override
    public void run() {
        statLog.info("at least it works.");
        while (true) {
            try {
                Thread.sleep(Constants.STATISTIC_REFRESH_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.logStats();
        }
    }

//    private void sendStatistic() {
//        try {
//            Socket socket = new Socket(Constants.MONITOR_HOST, Constants.MONITOR_PORT);
//            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
//            objectOutputStream.writeObject(realtimeStatistic);
//            objectOutputStream.flush();
//            objectOutputStream.close();
//            socket.close();
//        } catch (IOException e) {
//            logger.error("Statistic while sending Statistic:\n{}", Prints.getPrintStackTrace(e));
//        }
//    }
}