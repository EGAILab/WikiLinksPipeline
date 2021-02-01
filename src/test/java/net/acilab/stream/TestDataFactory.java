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

  public String getFirstPointerFile() {
    return "F:\\Data\\wiki-links_google\\data-0-of-9_pointer.txt";
  }

  public String getFileNotExist() {
    return "F:\\Data\\wiki-links_google\\data-00-of-9";
  }

  public WikiLinksArticleEvent getFirstFileEventOne() {
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

  public WikiLinksArticleEvent getFirstFileEventTwo() {
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

  public WikiLinksArticleEvent getFirstFileEventThree() {
    Mention mention1 = Mention.newBuilder().setMentionstring("Microsoft").setByteoffset(80679)
        .setTargeturl("http://en.wikipedia.org/wiki/Microsoft").build();
    Mention mention2 = Mention.newBuilder().setMentionstring("Microsoft").setByteoffset(134415)
        .setTargeturl("http://en.wikipedia.org/wiki/Microsoft").build();
    Mention mention3 = Mention.newBuilder().setMentionstring("Windows Server 2008").setByteoffset(80862)
        .setTargeturl("http://en.wikipedia.org/wiki/Windows_Server_2008").build();
    Mention mention4 = Mention.newBuilder().setMentionstring("Windows Server 2008").setByteoffset(134744)
        .setTargeturl("http://en.wikipedia.org/wiki/Windows_Server_2008").build();
    Mention mention5 = Mention.newBuilder().setMentionstring("Windows 7").setByteoffset(81028)
        .setTargeturl("http://en.wikipedia.org/wiki/Windows_7").build();
    Mention mention6 = Mention.newBuilder().setMentionstring("Windows 7").setByteoffset(134910)
        .setTargeturl("http://en.wikipedia.org/wiki/Windows_7").build();
    Mention mention7 = Mention.newBuilder().setMentionstring("operating systems.").setByteoffset(81109)
        .setTargeturl("http://en.wikipedia.org/wiki/Operating_system").build();
    Mention mention8 = Mention.newBuilder().setMentionstring("Windows Vista").setByteoffset(134573)
        .setTargeturl("http://en.wikipedia.org/wiki/Windows_Vista").build();

    Token token1 = Token.newBuilder().setTokenstring("Fresh").setByteoffset(54828).build();
    Token token2 = Token.newBuilder().setTokenstring("evidence").setByteoffset(32081).build();
    Token token3 = Token.newBuilder().setTokenstring("Allow").setByteoffset(72597).build();
    Token token4 = Token.newBuilder().setTokenstring("operator").setByteoffset(148693).build();
    Token token5 = Token.newBuilder().setTokenstring("notice").setByteoffset(507684).build();
    Token token6 = Token.newBuilder().setTokenstring("save").setByteoffset(77567).build();
    Token token7 = Token.newBuilder().setTokenstring("subfolder").setByteoffset(154988).build();
    Token token8 = Token.newBuilder().setTokenstring("PELCO").setByteoffset(490470).build();
    Token token9 = Token.newBuilder().setTokenstring("crashed").setByteoffset(301434).build();
    Token token10 = Token.newBuilder().setTokenstring("audit").setByteoffset(296060).build();

    WikiLinksArticleEvent event = WikiLinksArticleEvent.newBuilder().setUrl(
        "ftp://38.107.129.5/Training/Training%20Documentation/Latitude%20V6.2%20Training%20Binder/06%20Latitude%206%202%20Release%20Notes_Build%2027.pdf")
        .setMentions(Arrays.asList(mention1, mention2, mention3, mention4, mention5, mention6, mention7, mention8))
        .setTokens(Arrays.asList(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10))
        .build();

    return event;
  }

  public WikiLinksArticleEvent getFirstFileEventFour() {
    Mention mention1 = Mention.newBuilder().setMentionstring("bootstrapping").setByteoffset(598297)
        .setTargeturl("http://en.wikipedia.org/wiki/Bootstrapping").build();

    Token token1 = Token.newBuilder().setTokenstring("reducing").setByteoffset(518133).build();
    Token token2 = Token.newBuilder().setTokenstring("linked").setByteoffset(68739).build();
    Token token3 = Token.newBuilder().setTokenstring("locked").setByteoffset(338757).build();
    Token token4 = Token.newBuilder().setTokenstring("input").setByteoffset(303071).build();
    Token token5 = Token.newBuilder().setTokenstring("named").setByteoffset(391287).build();
    Token token6 = Token.newBuilder().setTokenstring("would").setByteoffset(517889).build();
    Token token7 = Token.newBuilder().setTokenstring("red").setByteoffset(550911).build();
    Token token8 = Token.newBuilder().setTokenstring("ROM").setByteoffset(85363).build();
    Token token9 = Token.newBuilder().setTokenstring("Link").setByteoffset(414545).build();
    Token token10 = Token.newBuilder().setTokenstring("globally").setByteoffset(359504).build();

    WikiLinksArticleEvent event = WikiLinksArticleEvent.newBuilder().setUrl(
        "ftp://Autoidread:read@ftp.rrc.ru/!!!Motorola/Motorola%20MSP%20SCHOOL/Moscow%20Oct%202010/student/091102_MCD_Class.pdf")
        .setMentions(Arrays.asList(mention1))
        .setTokens(Arrays.asList(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10))
        .build();

    return event;
  }

  public WikiLinksArticleEvent getFirstFileLastEvent() {
    Mention mention1 = Mention.newBuilder().setMentionstring("bootstrapping").setByteoffset(12382)
        .setTargeturl("http://en.wikipedia.org/wiki/Auto-Tune").build();

    Token token1 = Token.newBuilder().setTokenstring("reality").setByteoffset(8435).build();
    Token token2 = Token.newBuilder().setTokenstring("haven").setByteoffset(8529).build();
    Token token3 = Token.newBuilder().setTokenstring("non").setByteoffset(8482).build();
    Token token4 = Token.newBuilder().setTokenstring("link").setByteoffset(13184).build();
    Token token5 = Token.newBuilder().setTokenstring("night").setByteoffset(8807).build();
    Token token6 = Token.newBuilder().setTokenstring("alum").setByteoffset(12205).build();
    Token token7 = Token.newBuilder().setTokenstring("twitter").setByteoffset(13390).build();
    Token token8 = Token.newBuilder().setTokenstring("Spears").setByteoffset(8794).build();
    Token token9 = Token.newBuilder().setTokenstring("think").setByteoffset(12936).build();
    Token token10 = Token.newBuilder().setTokenstring("Pratt").setByteoffset(12926).build();

    WikiLinksArticleEvent event = WikiLinksArticleEvent.newBuilder()
        .setUrl("https://zzzlist.wordpress.com/tag/heidi-montag-spencer-pratt/").setMentions(Arrays.asList(mention1))
        .setTokens(Arrays.asList(token1, token2, token3, token4, token5, token6, token7, token8, token9, token10))
        .build();

    return event;
  }

  public long[] getFirstFileOffsets() {
    long[] offsets = { 0l, 349l, 677l, 1597l, 1984l, 2421l, 6971l };
    return offsets;
  }

  public long[] getFirstFileEndOfFileOffsets() {
    long[] offsets = { 592017993l, 59207685l, 592017302 };
    return offsets;
  }

  public long getFirstFileCurrentOffset() throws Exception {
    String firstPointerFile = "F:\\Data\\wiki-links_google\\data-0-of-9_pointer.txt";
    BufferedReader bufferReader;

    bufferReader = new BufferedReader(new FileReader(firstPointerFile));
    long offset = Long.valueOf(bufferReader.readLine());
    bufferReader.close();
    return offset;
  }
}
