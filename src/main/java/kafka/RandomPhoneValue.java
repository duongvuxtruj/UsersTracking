package kafka;

import com.schema.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.text.DecimalFormat;
import java.util.*;

public class RandomPhoneValue {
    private Random rand = new Random();
    private java.util.Properties props = new Properties();

    private Producer<byte[], byte[]> kafkaProducer;



    private void initPropertiesProducer()
    {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Producer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    }


//    public String randomPhone()
//    {
//        int num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8);
//        int num2 = rand.nextInt(743);
//        int num3 = rand.nextInt(10000);
//
//        DecimalFormat df3 = new DecimalFormat("000"); // 3 zeros
//        DecimalFormat df4 = new DecimalFormat("0000"); // 4 zeros
//
//        String phoneNumber = df3.format(num1) + "-" + df3.format(num2) + "-" + df4.format(num3);
//
//        return phoneNumber;
//
//    }
    public Set<String> randomPhone() {
        Set<String> phoneNumbers = new HashSet<>();
        DecimalFormat df3 = new DecimalFormat("000"); // 3 zeros
        DecimalFormat df4 = new DecimalFormat("0000"); // 4 zeros

        int numberOfPhones = 5000;
        while (phoneNumbers.size() < numberOfPhones) {
            int num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8);
            int num2 = rand.nextInt(743);
            int num3 = rand.nextInt(10000);

            String phoneNumber = df3.format(num1) + "-" + df3.format(num2) + "-" + df4.format(num3);
            phoneNumbers.add(phoneNumber);
        }

        return phoneNumbers;
    }
    public Integer randomLongLat()
    {
        int min = -100000;
        int max = 100000;
        int random_int = (int)Math.floor(Math.random()*(max-min+1)+min);

        return random_int;
    }


//    public String randomUserId()
//    {
//        int min = 1;
//        int max = 10000;
//        int random_int = (int)Math.floor(Math.random()*(max-min+1)+min);
//        String result = Integer.toString(random_int);
//        return result;
//    }

    public byte[] randomKey()
    {
        int min = -100000;
        int max = 100000;
        int random_int = (int)Math.floor(Math.random()*(max-min+1)+min);
        byte[] random_byte = Integer.toString(random_int).getBytes();

        return random_byte;
    }

    public Message.UserLocation getRandomMessage()
    {
        Message.UserLocation userLocation = Message.UserLocation.newBuilder()
                .setPhone(this.randomPhone().toString())
                .setLat(this.randomLongLat())
                .setLon(this.randomLongLat())
                .setTimestamp(System.currentTimeMillis())
                .build();
//        HashMap<String, String> test = new HashMap<>();

        return userLocation;
    }

    public List<Message.UserLocation> getRandomListMessage(int number_message)
    {
        List<Message.UserLocation> list = new ArrayList<>(10);

        for(int i = 0; i<number_message; i++)
        {
            Message.UserLocation userLocation = this.getRandomMessage();
            list.add(userLocation);
        }

        return list;
    }

    public void sendMessage(int number_message, String topic_name)
    {
        this.initPropertiesProducer();
        this.kafkaProducer = new KafkaProducer<byte[], byte[]>(this.props);
        // Send messages to multiple partitions
        int[] partitions = {0, 1, 2};  // Specify the desired partitions

        for (int i = 0; i < number_message; i++)
        {
            String key = Integer.toString(i);  // Use a unique key for each message
            int partition = partitions[i%3];
            this.kafkaProducer.send(new ProducerRecord<>(topic_name, partition, this.randomKey(), this.getRandomMessage().toByteArray()));
        }
        this.kafkaProducer.close();
    }
}
