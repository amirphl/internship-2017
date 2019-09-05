package utils;

public class Constants {

    public final static int PARSER_NUMBER = 2; //40
    public final static int FETCHER_NUMBER = 4; // 128
    public final static int LRU_TIME_LIMIT = 30;
    public final static int FETCH_TIMEOUT = 1000;
    public final static String URL_TOPIC = "la";
    public final static int STATISTIC_REFRESH_TIME = 10000;

    public final static int FETCHED_DATA_QUEUE_SIZE = 2000;
    public final static int URL_QUEUE_SIZE = 2000;

    public final static int KAFKA_SLEEP_TIME = 20;

    public final static String MONITOR_HOST = "localhost";
    public final static int MONITOR_PORT = 7788;


    //HBase Constants
    public final static String TABLE_NAME = "CrawlerTable";
    public final static String FAMILY_NAME = "data";
    public final static String SEPARATOR = ";|#";
    public final static String COLUMN_QUALIFIER_NAME_1 = "innerLinks";
    public final static String COLUMN_QUALIFIER_NAME_2 = "anchors";
    public final static int HBASE_PUT_BATCH_NUMBER = 2;//500

    //ElasticSearch Constants
    public static final String HOST_1 = "localhost";
    public static final String HOST_2 = "localhost";
    public static final int PORT_ONE = 9200;
    public static final int PORT_TWO = 9201;
    public static final String SCHEME = "http";
    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String TITLE = "title";
    public static final String CONTENT = "content";
    public static final String PRSCORE = "prscore";
    public static final String ANCHOR_1 = "anchor1";
    public static final String ANCHOR_2 = "anchor2";
    public static final String ANCHOR_3 = "anchor3";
    public static final String ANCHOR_4 = "anchor4";
    public static final String ANCHOR_5 = "anchor5";
    public static final int ELASTIC_URL_SIZE_LIMIT = 512;
}
