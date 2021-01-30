package net.acilab.stream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.acilab.stream.processor.wikilinks.serialization.Mention;
import net.acilab.stream.processor.wikilinks.serialization.Token;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;
import net.acilab.stream.utils.TestApplicationConstants;

@Component
public class TestDataFactory {

  /** Configurations **/

  public List<String> getEventFileList() {
    return TestApplicationConstants.EVENT_FILE_LIST;
    // String str[] = new String[] { "F:\\Data\\wiki-links_google\\data-0-of-9",
    // "F:\\Data\\wiki-links_google\\data-1-of-9",
    // "F:\\Data\\wiki-links_google\\data-2-of-9",
    // "F:\\Data\\wiki-links_google\\data-3-of-9",
    // "F:\\Data\\wiki-links_google\\data-4-of-9",
    // "F:\\Data\\wiki-links_google\\data-5-of-9",
    // "F:\\Data\\wiki-links_google\\data-6-of-9",
    // "F:\\Data\\wiki-links_google\\data-7-of-9",
    // "F:\\Data\\wiki-links_google\\data-8-of-9",
    // "F:\\Data\\wiki-links_google\\data-9-of-9" };
    // return Arrays.asList(str);
  }

  public List<String> getEventPointerFileList() {
    return TestApplicationConstants.EVENT_POINTER_FILE_LIST;
    // String str[] = new String[] {
    // "F:\\Data\\wiki-links_google\\data-0-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-1-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-2-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-3-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-4-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-5-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-6-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-7-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-8-of-9_pointer.txt",
    // "F:\\Data\\wiki-links_google\\data-9-of-9_pointer.txt" };
    // return Arrays.asList(str);
  }

  public Properties getThroughputProducerProperties() {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9092,broker-2:9093,broker-3:9094");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    // High Throughput config
    config.put(ProducerConfig.ACKS_CONFIG, "0");
    config.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
    return config;
  }

  public String getTopic() {
    return "acilab.nlp.wikilinks.origin.1";
  }

  /** For WikiLinksEventFileProcessorTest **/

  public String getFirstFile() {
    return "F:\\Data\\wiki-links_google\\data-0-of-9";
  }

  public String getFileNotExist() {
    return "F:\\Data\\wiki-links_google\\data-00-of-9";
  }

  public WikiLinksArticleEvent getFirstFileFirstEvent() {
    Mention mention1 = Mention.newBuilder().setMentionstring("r NetBIOS").setByteoffset(176937)
        .setTargeturl("http://en.wikipedia.org/wiki/NetBIOS").build();

    Token token1 = Token.newBuilder().setTokenstring("acting").setByteoffset(304940).build();
    Token token2 = Token.newBuilder().setTokenstring("whose").setByteoffset(247626).build();
    Token token3 = Token.newBuilder().setTokenstring("capabilities").setByteoffset(70039).build();
    Token token4 = Token.newBuilder().setTokenstring("ME").setByteoffset(201398).build();
    Token token5 = Token.newBuilder().setTokenstring("calls").setByteoffset(514390).build();
    Token token6 = Token.newBuilder().setTokenstring("preferably").setByteoffset(346689).build();
    Token token7 = Token.newBuilder().setTokenstring("functionality").setByteoffset(358183).build();
    Token token8 = Token.newBuilder().setTokenstring("anything").setByteoffset(7034).build();
    Token token9 = Token.newBuilder().setTokenstring("boost").setByteoffset(508294).build();
    Token token10 = Token.newBuilder().setTokenstring("enjoying").setByteoffset(211878).build();

    WikiLinksArticleEvent event = WikiLinksArticleEvent.newBuilder()
        .setUrl("ftp://212.18.29.48/ftp/pub/allnet/nas/all60300/ALL60300_UM_V12_EN.pdf")
        .setMentions(Arrays.asList(mention1))
        .setTokens(Arrays.asList(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10))
        .build();

    return event;
  }

  public String getFirstFileSecondEventOffset() {
    return "349";
  }

  public WikiLinksArticleEvent getFirstFileSecondEvent() {
    Mention mention1 = Mention.newBuilder().setMentionstring("NetBIOS").setByteoffset(176937)
        .setTargeturl("http://en.wikipedia.org/wiki/NetBIOS").build();

    Token token1 = Token.newBuilder().setTokenstring("navigate").setByteoffset(393103).build();
    Token token2 = Token.newBuilder().setTokenstring("Whether").setByteoffset(591729).build();
    Token token3 = Token.newBuilder().setTokenstring("judgment").setByteoffset(605155).build();
    Token token4 = Token.newBuilder().setTokenstring("overlap").setByteoffset(199643).build();
    Token token5 = Token.newBuilder().setTokenstring("Portals").setByteoffset(434138).build();
    Token token6 = Token.newBuilder().setTokenstring("D").setByteoffset(119657).build();
    Token token7 = Token.newBuilder().setTokenstring("IF").setByteoffset(614676).build();
    Token token8 = Token.newBuilder().setTokenstring("Selections").setByteoffset(212222).build();
    Token token9 = Token.newBuilder().setTokenstring("heat").setByteoffset(7764).build();
    Token token10 = Token.newBuilder().setTokenstring("LOSS").setByteoffset(614305).build();

    WikiLinksArticleEvent event = WikiLinksArticleEvent.newBuilder()
        .setUrl("ftp://212.18.29.48/ftp/pub/allnet/nas/all60600/ALL60600_UM_EN.pdf")
        .setMentions(Arrays.asList(mention1))
        .setTokens(Arrays.asList(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10))
        .build();

    return event;
  }

  public String getFirstFileEndOfFileOffset() {
    return "592017993";
  }

  public String getFirstFileCurrentOffset() throws Exception {
    String firstPointerFile = "F:\\Data\\wiki-links_google\\data-0-of-9_pointer.txt";
    BufferedReader bufferReader;

    bufferReader = new BufferedReader(new FileReader(firstPointerFile));
    String offset = bufferReader.readLine();
    bufferReader.close();
    return offset;
  }
}
