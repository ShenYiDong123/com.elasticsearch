package com.example.demo.index;

import com.fasterxml.jackson.databind.util.ArrayBuilders;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class QueryDoc {

    /**
     * 创建RestHighLevelClient客户端
     * @return
     */
    private RestHighLevelClient getRestHighLevelClient() {
        HttpHost[] httpHostArray = new HttpHost[1];
        httpHostArray[0] = new HttpHost("127.0.0.1",9200, "http");
        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }

    /**
     * 查询文档
     * es原文：
     * http://localhost:9200/xc_course/_doc/_search
     * {"query":{"match_all":{}},"_source":["name","studymodel"]}
     * @throws IOException
     */
    @Test
    public void testSearchAll() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }

    /**
     * 分页查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"from":0,"size":1,"query":{"match_all":{}},"_source":["name","studymodel"]}
     * @throws IOException
     */
    @Test
    public void testSearchLimit() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置分页
        //设置起始下标
        searchSourceBuilder.from(0);
        //每页显示个数
        searchSourceBuilder.size(10);

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }

    /**
     * 匹配查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"query":{"term":{"name":"spring"}},"_source":["name","studymodel"]}
     * @throws IOException
     */
    @Test
    public void testSearchTerm() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置匹配查询
        searchSourceBuilder.query(QueryBuilders.termQuery("name","spring"));
        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }

    /**
     * 根据ID匹配查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"query":{"ids":{"values":["3","4","100"]}}}
     * @throws IOException
     */
    @Test
    public void testSearchById() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String[]split=new String[]{"1","2"};
        List<String> idList = Arrays.asList(split);
        searchSourceBuilder.query(QueryBuilders.termsQuery("_id",idList));
        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * match query(分词查询)
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"query":{"match":{"description":{"query":"spring开发","operator":"or"}}}}
     * @throws IOException
     */
    @Test
    public void testSearchMatch() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //分词匹配关键字
        searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发").operator(Operator.OR));

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * minimum_should_match(分词占比查询)
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"query":{"match":{"description":{"query":"spring开发","minimum_should_match":"80%"}}}}
     * @throws IOException
     */
    @Test
    public void testSearchminimumShouldMatch() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //分词匹配关键字
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("description","前台页面开发框架架构").minimumShouldMatch("80%");
        //设置匹配占比
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder);

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * multi_match(多映射，权重查询)
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"query":{"match":{"description":{"query":"spring开发","minimum_should_match":"80%"}}}}
     * @throws IOException
     */
    @Test
    public void testSearchMultiMatch() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 查询所有文档
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring框架","name","description")
                .minimumShouldMatch("50%");
        multiMatchQueryBuilder.field("name",10); //提升boost
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(multiMatchQueryBuilder);

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * 布尔查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"_source":["name","studymodel","description"],"from":0,"size":1,"query":{"bool":{"must":[{"multi_match":{"query":"spring框架","minimum_should_match":"50%","fields":["name^10","description"]}},{"term":{"studymodel":"201001"}}]}}}
     * @throws IOException
     */
    @Test
    public void testSearchBool() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 创建搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //创建搜索源配置对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","pic","studymodel"},new String[]{});

        //multiQuery
        String keyword="spring开发框架";
        MultiMatchQueryBuilder multiMatchQueryBuilder=QueryBuilders.multiMatchQuery("spring框架","name","description").minimumShouldMatch("50%");multiMatchQueryBuilder.field("name",10);

        //TermQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);


        //设置布尔查询d对象
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);


        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * 过滤器查询（range和term一次只能对一个Field设置范围过虑。）
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"_source":["name","studymodel","description"],"from":0,"size":1,"query":{"bool":{"must":[{"multi_match":{"query":"spring框架","minimum_should_match":"50%","fields":["name^10","description"]}},{"term":{"studymodel":"201001"}}]}}}
     * @throws IOException
     */
    @Test
    public void testSearchFilter() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 创建搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //创建搜索源配置对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","pic","studymodel"},new String[]{});

        //multiQuery
        String keyword="spring开发框架";
        MultiMatchQueryBuilder multiMatchQueryBuilder=QueryBuilders.multiMatchQuery("spring框架","name","description").minimumShouldMatch("50%");multiMatchQueryBuilder.field("name",10);

        //TermQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel","201001"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));

        //设置布尔查询d对象
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);


        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * 排序查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"_source":["name","studymodel","description","price"],"query":{"bool":{"filter":[{"range":{"price":{"gte":0,"lte":100}}}]}},"sort":[{"studymodel":"desc"},{"price":"asc"}]}
     * @throws IOException
     */
    @Test
    public void testSearchSort() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 创建搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //创建搜索源配置对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","pic","studymodel"},new String[]{});

        //multiQuery
        String keyword="spring开发框架";
        MultiMatchQueryBuilder multiMatchQueryBuilder=QueryBuilders.multiMatchQuery("spring框架","name","description").minimumShouldMatch("50%");multiMatchQueryBuilder.field("name",10);

        //TermQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel","201001"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));

        //设置布尔查询d对象
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        //排序
        searchSourceBuilder.sort(new FieldSortBuilder("studymodel").order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    /**
     * 高亮查询
     * es原文：
     * post http://localhost:9200/xc_course/doc/_search
     * {"_source":["name","studymodel","description","price"],"query":{"bool":{"must":[{"multi_match":{"query":"开发框架","minimum_should_match":"50%","fields":["name^10","description"],"type":"best_fields"}}],"filter":[{"range":{"price":{"gte":0,"lte":100}}}]}},"sort":[{"price":"asc"}],"highlight":{"pre_tags":["<tag1>"],"post_tags":["</tag2>"],"fields":{"name":{},"description":{}}}}
     * @throws IOException
     */
    @Test
    public void testSearchHighLight() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        // 创建搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        searchRequest.types("_doc");


        //创建搜索源配置对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","pic","studymodel"},new String[]{});

        //multiQuery
        String keyword="spring开发框架";
        MultiMatchQueryBuilder multiMatchQueryBuilder=QueryBuilders.multiMatchQuery("spring框架","name","description").minimumShouldMatch("50%");multiMatchQueryBuilder.field("name",10);

        //TermQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel","201001"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));

        //设置布尔查询d对象
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        //排序
        searchSourceBuilder.sort(new FieldSortBuilder("studymodel").order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));

        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>"); //设置前缀
        highlightBuilder.postTags("</tag>"); //设置后缀

        //设置高亮字段
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
        highlightBuilder.fields().add(new HighlightBuilder.Field("description"));
        searchSourceBuilder.highlighter(highlightBuilder);

        //source源字段过滤
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index=hit.getIndex();
            String type=hit.getType();
            String id=hit.getId();
            float score=hit.getScore();
            String sourceAsString=hit.getSourceAsString();
            Map<String,Object> sourceAsMap=hit.getSourceAsMap();
            String name=(String)sourceAsMap.get("name");
            String studymodel =(String)sourceAsMap.get("studymodel");
            String description=(String)sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }
}
