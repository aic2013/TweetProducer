package aic2013.tweetproducer;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Producer extends Thread {

    private volatile boolean running = true;

    private final MongoClient mongoClient;
    private final String mongoDatabase;
    private final String mongoCollection;
    private final ConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final MessageProducer extractionProducer;
    private final MessageProducer followsProducer;
    private final MessageProducer followerFetcherProducer;

    private static final String BROKER_URL = "tcp://localhost:61616";
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

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        MongoClient mongoClient = new MongoClient(mongoUrl);

        Producer producer = new Producer(mongoClient, mongoDatabase, mongoCollection, factory, extractionQueueName, followsQueueName, followerFetcherQueueName);
        producer.start();

        System.out.println("Started producer with the following configuration:");
        System.out.println("\tBroker: " + brokerUrl);
        System.out.println("\t\tExtraction queue name: " + extractionQueueName);
        System.out.println("\t\tFollows queue name: " + followsQueueName);
        System.out.println("\tMongo: " + mongoUrl);
        System.out.println("\t\tDatabase: " + mongoDatabase);
        System.out.println("\t\tCollection: " + mongoCollection);
        System.out.println();
        System.out.println("To shutdown the application please type 'exit'.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String command;

        while ((command = br.readLine()) != null) {
            if ("exit".equals(command)) {
                break;
            }
        }

        producer.shutdown();
    }

    public Producer(MongoClient mongoClient, String mongoDatabase, String mongoCollection, ConnectionFactory factory, String extractionQueueName, String followsQueueName, String followerFetcherQueueName)
        throws JMSException {
        this.mongoClient = mongoClient;
        this.mongoDatabase = mongoDatabase;
        this.mongoCollection = mongoCollection;
        this.factory = factory;
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        extractionProducer = session.createProducer(session.createQueue(extractionQueueName));
        followsProducer = session.createProducer(session.createQueue(followsQueueName));
        followerFetcherProducer = session.createProducer(session.createQueue(followerFetcherQueueName));
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        try {
            DB db = mongoClient.getDB(mongoDatabase);
            DBCollection statusCollection = db.getCollection(mongoCollection);
            DBCursor cursor = statusCollection.find();

            while (running && cursor.hasNext()) {
                try {
                    TextMessage message = session.createTextMessage(cursor.next()
                        .toString());
                    extractionProducer.send(message);
                    followsProducer.send(message);
                } catch (JMSException ex) {
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
                connection.close();
            } catch (JMSException ex) {
                Logger.getLogger(Producer.class.getName())
                    .log(Level.SEVERE, null, ex);
            }
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
