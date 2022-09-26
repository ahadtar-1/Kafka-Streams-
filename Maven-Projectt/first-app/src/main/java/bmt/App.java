package bmt;
//Main-Class: com.package.bmt;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.PropertyConfigurator;
import java.io.FileInputStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.json.simple.JSONObject;



public class App 
{
    public static void main( String[] args )
    {
        //System.out.println( "Hello World!" );
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

       try {
           //reading the log4j properties files
           props.load(new FileInputStream("/home/ahad/Desktop/Maven-Project/first-app/src/main/java/resources/log4j.properties/"));
           //PropertyConfigurator.configure(props);
       } catch (Exception e) {
          System.out.println(e.toString());
       }

      // setting the properties in the log4j configurator
       PropertyConfigurator.configure(props);

       //final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
       //final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
       //final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer,jsonNodeDeserializer);

        String inputTopic = "data_topic";
        String OutTopic = "out_topic";

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic)
               .peek((key, value) -> System.out.println((JSONObject) value))
               .mapValues((value) -> create_new((JSONObject) value))
               .to(OutTopic);
         
        String dir = System.getProperty("user.dir");   
        System.out.println(dir); 
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
  
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }

   // private static JsonNode setnewNode(JsonNode oldNode, JsonNode newN){
     //   ObjectNode finalNode = JsonNodeFactory.instance.objectNode();
     //   finalNode.put("name",oldNode.get("name"));
     //   finalNode.put("competency",oldNode.get("competency"));

      //  return finalNode;

   // }

   private static JSONObject create_new(JSONObject node) {

    JSONObject obj = new JSONObject();
    obj.put("name",node.get("name"));
    obj.put("competency",node.get("competency"));

    return obj;

}

}
