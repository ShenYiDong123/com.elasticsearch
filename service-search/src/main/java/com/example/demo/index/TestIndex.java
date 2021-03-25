package com.example.demo.index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 索引实现类
 * 备注：索引等于数据库
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {

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
     * 创建索引
     * es原文：
     * put http://localhost:9200/索引名称
     * {"settings":{"index":{"number_of_shards":1,#分片的数量"number_of_replicas":0#副本数量}}}
     *
     * es原文(创建映射)：
     * http://localhost:9200/xc_course/doc/_mapping
     * {"properties":{"name":{"type":"text","analyzer":"ik_max_word","search_analyzer":"ik_smart"},"description":{"type":"text","analyzer":"ik_max_word","search_analyzer":"ik_smart"},"studymodel":{"type":"keyword"},"price":{"type":"float"},"timestamp":{"type":"date","format":"yyyy‐MM‐ddHH:mm:ss||yyyy‐MM‐dd||epoch_millis"}}}
     */
    @Test
    public void testCreateIndex() throws IOException{
        //创建索引请求对象，并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("xc_course");
        //设置索引参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards",1).put("number_of_replicas",0));

        //设置映射
        String mappingStr = "{\"properties\":{\"description\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\",\"search_analyzer\":\"ik_smart\"},\"name\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\",\"search_analyzer\":\"ik_smart\"},\"pic\":{\"type\":\"text\",\"index\":false},\"price\":{\"type\":\"float\"},\"studymodel\":{\"type\":\"keyword\"},\"timestamp\":{\"type\":\"date\",\"format\":\"yyyy‐MM‐ddHH:mm:ss||yyyy‐MM‐dd||epoch_millis\"}}}";
        createIndexRequest.mapping("_doc",mappingStr, XContentType.JSON);

        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //创建索引操作客户端
        IndicesClient indices = client.indices();

        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
        //得到响应结果
        System.out.println(createIndexResponse.isAcknowledged());
    }

    /**
     * 删除索引
     * es原文：
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //生成删除索引请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("xc_course");
        //删除索引
        DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest);
        //打印结果
        System.out.println(deleteIndexResponse.isAcknowledged());

    }

    /**
     * 添加文档
     * es原文：
     * put http://localhost:9200/xc_course/doc/3
     * {"name":"springcloud实战","description":"本课程主要从四个章节进行讲解：1.微服务架构入门2.springcloud基础入门3.实战SpringBoot4.注册中心eureka。","studymodel":"201001""price":5.6}
     * @throws IOException
     */
    @Test
    public void testAddDoc() throws IOException{
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //准备map数据
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("name","springcloud实战");
        jsonMap.put("description","本课程主要从四个章节进行讲解：1.微服务架构入门2.springcloud基础入门3.实战SpringBoot4.注册中心eureka。");
        jsonMap.put("studymodel","201001");
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy‐MM‐ddHH:mm:ss");
        jsonMap.put("timestamp",dateFormat.format(new Date()));
        jsonMap.put("price",5.6f);

        //索引请求对象
        IndexRequest indexRequest = new IndexRequest("xc_course" , "_doc");
        //指定索引文档内容
        indexRequest.source(jsonMap);
        //索引响应对象
        IndexResponse indexResponse = client.index(indexRequest);
        //获取响应内容
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result);
    }


    /**
     * 查询文档
     * es原文：
     * GET /{index}/{type}/{id}
     *
     * @throws IOException
     */
    @Test
    public void getDoc() throws IOException{
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        GetRequest getRequest = new GetRequest("xc_course","_doc","eMQLaHgBmNJHUm7ZcCy4");
        GetResponse getResponse = client.get(getRequest);
        boolean exists = getResponse.isExists();
        Map<String,Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);
    }

    /**
     * 更新文档
     * es原文：
     * *完全替换
     * Post：http://localhost:9200/xc_test/doc/3
     * {"name":"springcloud实战","description":"本课程主要从四个章节进行讲解：1.微服务架构入门2.springcloud基础入门3.实战SpringBoot4.注册中心eureka。","studymodel":"201001""price":5.6}
     *
     * *局部更新
     * post: http://localhost:9200/xc_test/doc/3/_update
     * {"doc":{"price":66.6}}
     * @throws IOException
     */
    @Test
    public void updateDoc() throws IOException{
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        UpdateRequest updateRequest = new UpdateRequest("xc_course","_doc","eMQLaHgBmNJHUm7ZcCy4");
        Map<String,String> map = new HashMap<>();
        map.put("name","springcloud实战");
        updateRequest.doc(map);
        UpdateResponse updateResponse = client.update(updateRequest);
        RestStatus status = updateResponse.status();
        System.out.println(status);
    }

    /**
     * 删除文档
     * ***根据id删除***：
     * DELETE /{index}/{type}/{id}
     * ***搜索匹配删除，将搜索出来的记录删除***
     * POST /{index}/{type}/_delete_by_query
     * {"query":{"term":{"studymodel":"201001"}}}
     * @throws IOException
     */
    @Test
    public void deleteDoc() throws IOException{
        //创建RestHighLevelClient客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //删除索引请求对象
        DeleteRequest deleteRequest = new DeleteRequest("xc_course","_doc","eMQLaHgBmNJHUm7ZcCy4" );
        //响应对象
        DeleteResponse deleteResponse = client.delete(deleteRequest);
        //打印结果
        System.out.println(deleteResponse.getResult());
    }

}
