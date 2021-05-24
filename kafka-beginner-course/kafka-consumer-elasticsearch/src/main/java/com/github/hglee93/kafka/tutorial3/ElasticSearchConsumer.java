package com.github.hglee93.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final static String elasticPropertiesPath = "twitterApi.properties";

    private final static RestHighLevelClient restHighLevelClient = createClient();

    public static RestHighLevelClient createClient() {

        Properties properties = new Properties();

        String hostname = "";
        String username = "";
        String password = "";

        try {
            FileReader resources= new FileReader("./src/main/resources" + elasticPropertiesPath);
            properties.load(resources);

            hostname = properties.getProperty("hostname");
            username = properties.getProperty("username");
            password = properties.getProperty("password");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }


        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestHighLevelClient rhlc = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, 443, "https"))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                return httpAsyncClientBuilder.setDefaultCredentialsProvider(cp);
                            }
                        }));

        return rhlc;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elastic2";

        // Create
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    /***
     * 아래와 같이 elasticsearch의 인덱스를 생성할 경우,
     * 중복된 메시지를 처리할 때, 같은 메시지 결과가 중복으로 발생한다(다른 ID를 가진)
     * -> At least once 전략의 경우, 메시지 처리가 멱등성을 보장하도록 해야한다.
     * -> 여기서는 같은 메시지를 처리한다면 elasticsearch docId도 같은 id를 가져야 한다(멱등성)
     */
    public static String requestIndex(ConsumerRecord<String, String> record, String index, String docType) throws IOException {

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                .source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getId();
    }

    private static JsonParser jsonParser = new JsonParser();


    private static String extractFromTweetId(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    /***
     * 아래와 같이 elasticsearch의 docId를 tweetId로 설정함으로써
     * 멱등성을 보장함.
     */
    public static String requestIndexWithIdempotent(ConsumerRecord<String, String> record, String index, String docType) throws IOException {

        String id = extractFromTweetId(record.value());

        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets",
                id
                ).source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        return indexResponse.getId();
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");
            for(ConsumerRecord<String, String> record : records) {

                //String id = requestIndex(record, "twitter", "tweets");
                String id = requestIndexWithIdempotent(record, "twitter", "tweets");
                logger.info("id : " + id);

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Committing offset");
            consumer.commitSync();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //restHighLevelClient.close();
    }
}
