package aic2013.tweetproducer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Producer extends Thread {

    private volatile boolean running = true;

    private final MongoClient mongoClient;
    private final String mongoDatabase;
    private final String mongoCollection;
    private final ConnectionFactory factory;
    private final Connection connection;
    private final Channel channel;
    private final String extractionQueueName;
    private final String followsQueueName;
    private final String followerFetcherQueueName;

    private static final String BROKER_URL = "amqp://localhost:5672/virtualHost";
    private static final String MONGO_URL = "localhost";

    private static final String EXTRACTION_QUEUE_NAME = "tweet-extraction";
    private static final String FOLLOWS_QUEUE_NAME = "tweet-follows";
    private static final String FOLLOWER_FETCHER_QUEUE = "follower-fetcher";

    private static final String MONGO_DATABASE = "twitterdb";
    private static final String MONGO_COLLECTION = "statuses";

    private static String getProperty(String name, String defaultValue) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }
        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    public static void main(String[] args) throws Exception {
        String brokerUrl = getProperty("BROKER_URL", BROKER_URL);
        String mongoUrl = getProperty("MONGO_URL", MONGO_URL);
        String extractionQueueName = getProperty("EXTRACTION_QUEUE_NAME", EXTRACTION_QUEUE_NAME);
        String followsQueueName = getProperty("FOLLOWS_QUEUE_NAME", FOLLOWS_QUEUE_NAME);
        String followerFetcherQueueName = getProperty("FOLLOWER_FETCHER_QUEUE", FOLLOWER_FETCHER_QUEUE);
        String mongoDatabase = getProperty("MONGO_DATABASE", MONGO_DATABASE);
        String mongoCollection = getProperty("MONGO_COLLECTION", MONGO_COLLECTION);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(brokerUrl);
        MongoClient mongoClient = new MongoClient(mongoUrl);

        final Producer producer = new Producer(mongoClient, mongoDatabase, mongoCollection, factory, extractionQueueName, followsQueueName, followerFetcherQueueName);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (producer != null) {
                    producer.shutdown();
                }

                System.out.println("Exiting");
                System.exit(0);
            }
        });

        producer.start();

        System.out.println("Started producer with the following configuration:");
        System.out.println("\tBroker: " + brokerUrl);
        System.out.println("\t\tExtraction queue name: " + extractionQueueName);
        System.out.println("\t\tFollows queue name: " + followsQueueName);
        System.out.println("\t\tFollower Fetcher queue name: " + followerFetcherQueueName);
        System.out.println("\tMongo: " + mongoUrl);
        System.out.println("\t\tDatabase: " + mongoDatabase);
        System.out.println("\t\tCollection: " + mongoCollection);
        System.out.println();
        System.out.println("To shutdown the application please type 'exit'.");

        while (true) {
            Thread.sleep(1000);
        }
    }

    public Producer(MongoClient mongoClient, String mongoDatabase, String mongoCollection, ConnectionFactory factory, String extractionQueueName, String followsQueueName, String followerFetcherQueueName) throws IOException {
        this.mongoClient = mongoClient;
        this.mongoDatabase = mongoDatabase;
        this.mongoCollection = mongoCollection;
        this.factory = factory;
        this.extractionQueueName = extractionQueueName;
        this.followsQueueName = followsQueueName;
        this.followerFetcherQueueName = followerFetcherQueueName;

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(extractionQueueName, true, false, false, null);
        channel.queueDeclare(followsQueueName, true, false, false, null);
        channel.queueDeclare(followerFetcherQueueName, true, false, false, null);
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        try {
            DB db = mongoClient.getDB(mongoDatabase);
            DBCollection statusCollection = db.getCollection(mongoCollection);
            BasicDBObject query = new BasicDBObject("producer_processed", null);
            DBCursor cursor = statusCollection.find();

            while (running && cursor.hasNext()) {
                try {
                    DBObject tweet = cursor.next();
                    String message = tweet.toString();
                    channel.basicPublish("", extractionQueueName, null, message.getBytes());
                    channel.basicPublish("", followsQueueName, null, message.getBytes());
                    channel.basicPublish("", followerFetcherQueueName, null, message.getBytes());

                    tweet.put("producer_processed", true);
                    statusCollection.save(tweet);
                } catch (IOException ex) {
                    Logger.getLogger(Producer.class.getName())
                        .log(Level.SEVERE, null, ex);
                } catch (MongoException ex) {
                    Logger.getLogger(Producer.class.getName())
                        .log(Level.SEVERE, null, ex);
                } catch (RuntimeException ex) {
                    Logger.getLogger(Producer.class.getName())
                        .log(Level.SEVERE, null, ex);
                }
            }
        } finally {
            close();
        }
    }

    private void close() {
        if (connection != null) {
            try {
                channel.close();
                connection.close();
            } catch (IOException ex) {
                Logger.getLogger(Producer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
