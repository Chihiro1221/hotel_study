package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class HotelSearchTest {
    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 解析结果
        handleResponse(response);
    }

    /**
     * 解析es返回结果
     *
     * @param response
     */
    private static void handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        // 解析总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("查询出的总条数为为：" + total + "条");
        // 解析文档
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 解析高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields))
            {
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField != null)
                {
                    String name = highlightField.getFragments()[0].string();
                    hotelDoc.setName(name);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }

    @Test
    void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 解析结果
        handleResponse(response);
    }

    @Test
    void testBoolQueryMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city", "北京"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQuery);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 解析结果
        handleResponse(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        int page = 2, size = 5;
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        // 配置排序
        request.source().sort("price", SortOrder.ASC);
        // 配置分页
        request.source().from((page - 1) * size).size(size);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 解析结果
        handleResponse(response);
    }

    @Test
    void testMatchHighlight() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        // 设置match搜索
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 设置结果高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 解析结果
        handleResponse(response);
    }

    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.50.128:9200")
        ));
    }


    @AfterEach
    void afterEach() throws IOException {
        client.close();
    }
}
