package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult<HotelDoc> search(RequestParams params) {
        try {
            // 构造请求对象
            SearchRequest request = new SearchRequest("hotel");
            // 构造查询对象
            buildBasicQuery(params, request);
            // 配置分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);
            // 配置排序
            String location = params.getLocation();
            if (!StringUtils.isEmpty(location)) {
                request.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .unit(DistanceUnit.KILOMETERS)
                        .order(SortOrder.ASC)
                );
            }
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filter(RequestParams params) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().size(0);
            // 构造基本查询
            buildBasicQuery(params, request);
            // 设置聚合
            buildAggregation(request);
            // 解析结果
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 构造响应结果
            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            // 获取字段聚合结果
            ArrayList<String> brandAgg = getAggKeys(aggregations, "brandAgg");
            ArrayList<String> cityAgg = getAggKeys(aggregations, "cityAgg");
            ArrayList<String> starAgg = getAggKeys(aggregations, "starAgg");
            result.put("brand", brandAgg);
            result.put("city", cityAgg);
            result.put("starName", starAgg);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String key) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    new CompletionSuggestionBuilder("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 解析结果
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();

            if (CollectionUtils.isEmpty(options)) {
                return Collections.emptyList();
            }
            // 将options转换为结果集合
            List<String> keys = options.stream().map(option -> {
                return option.getText().string();
            }).collect(Collectors.toList());

            return keys;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据聚合字段获取聚合结果
     *
     * @param aggregations
     * @param aggName
     * @return
     */
    private static ArrayList<String> getAggKeys(Aggregations aggregations, String aggName) {
        ArrayList<String> keys = new ArrayList<>();
        // 获取字段结果
        Terms brandAgg = aggregations.get(aggName);
        // 获取桶
        List<? extends Terms.Bucket> buckets = brandAgg.getBuckets();
        if (!CollectionUtils.isEmpty(buckets)) {
            buckets.forEach(bucket -> {
                String key = bucket.getKeyAsString();
                keys.add(key);
            });
        }
        return keys;
    }

    private static void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders.terms("brandAgg").field("brand"));
        request.source().aggregation(AggregationBuilders.terms("cityAgg").field("city"));
        request.source().aggregation(AggregationBuilders.terms("starAgg").field("starName"));
    }

    /**
     * 构造基本查询对象
     *
     * @param param
     * @param request
     */
    private static void buildBasicQuery(RequestParams param, SearchRequest request) {
        String key = param.getKey();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 根据key查询
        if (StringUtils.isEmpty(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        // 城市查询
        String city = param.getCity();
        if (!StringUtils.isEmpty(city)) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        // 品牌查询
        String brand = param.getBrand();
        if (!StringUtils.isEmpty(brand)) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        // 星级查询
        String starName = param.getStarName();
        if (!StringUtils.isEmpty(starName)) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        // 价格范围查询
        Integer maxPrice = param.getMaxPrice();
        Integer minPrice = param.getMinPrice();
        if (maxPrice != null && minPrice != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
        }

        // 构建加分函数
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
                // 1.原始查询
                boolQuery,
                // 2.加分函数数组
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        // 加分函数
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                // 加分对象过滤
                                QueryBuilders.termQuery("isAD", true),
                                // 加分
                                ScoreFunctionBuilders.weightFactorFunction(5)
                        )
                }
        );
        request.source().query(functionScoreQuery);
    }

    /**
     * 解析es返回结果
     *
     * @param response
     */
    private static PageResult<HotelDoc> handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        // 解析总条数
        long total = searchHits.getTotalHits().value;
        // 解析文档
        ArrayList<HotelDoc> hotels = new ArrayList<HotelDoc>((int) total);
        SearchHit[] hits = searchHits.getHits();
        // 将json解析为hotelDoc
        for (SearchHit hit : hits) {
            // 获取源json
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        // 构造返回结果
        return new PageResult<>(total, hotels);
    }
}
