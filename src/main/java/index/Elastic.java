package index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static utils.Constants.*;

public class Elastic {
//    private static Settings settings = Settings.builder().put("cluster.name", "SearchEngine").build();
//    private static TransportClient client;

//    static {
//        try {
//            client = new PreBuiltTransportClient(settings)
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("176.31.***.177"), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("176.31.***.83"), 9300));
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }

    //    private ObjectMapper objectMapper = new ObjectMapper();
    private static RestHighLevelClient restHighLevelClient;
    private static Elastic instance;

    //singleton
    private Elastic() {
        makeConnection();
    }

    public static synchronized Elastic getInstance() {
        if (instance == null)
            instance = new Elastic();
        return instance;
    }

    private void makeConnection() {

        if (restHighLevelClient == null) {
            synchronized (Elastic.class) {
                restHighLevelClient = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(HOST_1, PORT_ONE, SCHEME),
                                new HttpHost(HOST_1, PORT_TWO, SCHEME)));
            }
        }

    }

    public synchronized void closeConnection() throws IOException {
        if (restHighLevelClient != null)
            restHighLevelClient.close();
        restHighLevelClient = null;
    }

    //todo remove deprecated
    public synchronized void indexData(String url, String content, String title) throws IOException {
        makeConnection(); //todo bad idea, used to prevent NullPointerException
        if (url.length() > ELASTIC_URL_SIZE_LIMIT)
            return;

//            XContentBuilder builder = jsonBuilder().startObject()
//                    .field("title", title)
//                    .field("content", content)
//                    .field("prscore", 0.0)
//                    .field("anchor1", "")
//                    .field("anchor2", "")
//                    .field("anchor3", "")
//                    .field("anchor4", "")
//                    .field("anchor5", "")
//                    .endObject();
//            CreateIndexRequest request = new CreateIndexRequest(INDEX);
//            request.mapping(builder);
//            CreateIndexResponse response = client.indices().create(request);
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put(TITLE, title);
        dataMap.put(CONTENT, content);
        dataMap.put(PRSCORE, "0");
        dataMap.put(ANCHOR_1, "");
        dataMap.put(ANCHOR_2, "");
        dataMap.put(ANCHOR_3, "");
        dataMap.put(ANCHOR_4, "");
        dataMap.put(ANCHOR_5, "");

        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, url).source(dataMap);
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        //IndexResponse response = client.prepareIndex(index, type, url).setSource(builder).get();
    }

    //todo remove deprecated
    public synchronized Map<String, Object> getData(String url) throws IOException {
        makeConnection(); //todo bad idea, used to prevent NullPointerException
        GetRequest getRequest = new GetRequest(INDEX, TYPE, url);
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
//        return getResponse != null ? objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;
        return getResponse != null ? getResponse.getSourceAsMap() : null;
//        GetResponse response = client.prepareGet(INDEX, TYPE, url)
//                .setOperationThreaded(false)
//                .get();
//        return response.getSourceAsString();
    }

    //todo what is this?
//    public SearchResponse searchData(String Str[]) {
//        SearchResponse response = client.prepareSearch(INDEX)
//                .setTypes(TYPE)
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.termQuery("message", Str[0]))                 // Query
//                //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
//                .setFrom(0).setSize(60).setExplain(true)
//                .get();
//
//        return response;
//    }

    //todo remove deprecated
    public synchronized void updateData(String url, String content, String title) throws IOException {
        makeConnection(); //todo bad idea, used to prevent NullPointerException
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, url);
        //.fetchSource(true);    // Fetch Object after its update
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field(TITLE, title)
                .field(CONTENT, content)
                //.field("ahmadScore", 0)//todo what is this?
                .field(PRSCORE, "0")
                .endObject());
        //client.update(updateRequest).get();
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
//        return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Person.class);
    }

    //todo remove deprecated
    public synchronized void deleteData(String url) throws IOException {
        makeConnection(); //todo bad idea, used to prevent NullPointerException
//        DeleteResponse response = client.prepareDelete(INDEX, TYPE, url).get();
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, url);
//        DeleteResponse deleteResponse =
        restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    //todo what is this?
//    public long DeleteDataByQ(String key, String Val) {
//        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//                        .filter(QueryBuilders.matchQuery(key, Val))
//                        .source("persons")
//                        .get();
//        return response.getDeleted();
//    }
}
