package org.jboss.eapqe.clouds.ec2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Queue;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    /**
     * See ConnectionFactoryProvider
     *
     * @param args
     * @throws JMSException
     */
    public static void main(String[] args) throws JMSException, CertificateException, NamingException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {

        String node = "localhost"; // "10.0.150.191";
        boolean security = false;

        if (args != null && args.length > 0) {
            node = args[0];
        }

        Main messagingClient = new Main();

        if (security) {
            messagingClient.messageSender(node, 8443,
                    "remoteclient",
                    "redhat",
                    "remoteclient",
                    "redhat",
                    "jms/queue/myQueue1",
                    "jms/RemoteConnectionFactoryPublicIP",
                    Paths.get("/tmp/client.ts"),
                    "redhat",
                    "jks",
                    1000);

            messagingClient.messageConsumer(node, 8443,
                    "remoteclient",
                    "redhat",
                    "remoteclient",
                    "redhat",
                    "jms/queue/myQueue1",
                    "jms/RemoteConnectionFactoryPublicIP",
                    Paths.get("/tmp/client.ts"),
                    "redhat",
                    "jks");
        } else {
            messagingClient.messageSender(node, 8080,
                    null,
                    null,
                    null,
                    null,
                    "jms/queue/myQueue1",
                    "jms/RemoteConnectionFactory",
                    null,
                    null,
                    null,
                    1000);

            messagingClient.messageConsumer(node, 8080,
                    null,
                    null,
                    null,
                    null,
                    "jms/queue/myQueue1",
                    "jms/RemoteConnectionFactory",
                    null,
                    null,
                    null);
        }
    }

    public void messageSender(String host, Integer port,
                              String remoteUser,
                              String remotePassword,
                              String brokerUser,
                              String brokerPassword,
                              String queueName,
                              String factoryName,
                              Path clientTrustStore,
                              String keyStorePassword,
                              String trustStoreType,
                              int numMessages) throws CertificateException, NamingException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException, JMSException {
        InitialContext context = getInitialContext(Collections.singletonMap(host, port), remoteUser, remotePassword, clientTrustStore, keyStorePassword, trustStoreType);
        ConnectionFactory cf = (ConnectionFactory) context.lookup(factoryName);
        Connection connection = (brokerUser != null && brokerPassword != null) ? cf.createConnection(brokerUser, brokerPassword) : cf.createConnection();
        connection.start();
        Session session = connection.createSession();
        Queue queue = (Queue) context.lookup(queueName);
        LOGGER.info("Queue name: " + queue.getQueueName());
        String messageBody = "Test message no.";
        LOGGER.info(new Date() + "Send messages \"" + messageBody + "\" ...");
        MessageProducer producer = session.createProducer(queue);

        System.out.println(new Date() + " - Sending "+numMessages+" messages ...");
        for (int i = 0; i < numMessages; i++) {
            producer.send(session.createTextMessage(messageBody + i));
            //System.out.println(new Date() + " Sent message with body \"" + messageBody + "\" ...");
        }
        System.out.println(new Date() + " - Sent "+numMessages+" messages.");

        producer.close();
        session.close();
    }

    public int messageConsumer(String host, Integer port,
                               String remoteUser,
                               String remotePassword,
                               String brokerUser,
                               String brokerPassword,
                               String queueName,
                               String factoryName,
                               Path clientTrustStore,
                               String keyStorePassword,
                               String trustStoreType) throws CertificateException, NamingException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException, JMSException {
        int numMessages = 0;
        InitialContext context = getInitialContext(Collections.singletonMap(host, port), remoteUser, remotePassword, clientTrustStore, keyStorePassword, trustStoreType);
        ConnectionFactory cf = (ConnectionFactory) context.lookup(factoryName);
        Connection connection = (brokerUser != null && brokerPassword != null) ? cf.createConnection(brokerUser, brokerPassword) : cf.createConnection();
        connection.start();
        Session session = connection.createSession();
        Queue queue3 = (Queue) context.lookup(queueName);
        LOGGER.info("Queue 1: " + queue3.getQueueName());
        MessageConsumer consumer = session.createConsumer(queue3);
        LOGGER.info("Receive timeout 240000 ...");
        System.out.println("Receive timeout 240000 ...");
        Message message = consumer.receive(240000); // message migration should start after 1000

        System.out.println(new Date() + " - Receiving messages ...");
        while (message != null) {
            numMessages++;
            //LOGGER.info("[3] Received message with body \"" + message.getBody(String.class) + "\" ...");
            //System.out.println(new Date() + " Received message with body \"" + message.getBody(String.class) + "\" ...");
            message = consumer.receive(1000);
        }
        System.out.println(new Date() + " - Received "+numMessages+" messages.");

        consumer.close();
        session.close();
        return numMessages;
    }

    private String formatProviderUrl(Map<String, Integer> nodes, boolean sslEnabled) {
        StringBuilder sb = new StringBuilder();
        int cnt = 1;
        for (Map.Entry<String, Integer> entry : nodes.entrySet()) {
            sb.append(
                    entry.getValue() == 8443 || sslEnabled ?
                            String.format("remote+https://%s:%d", entry.getKey(), entry.getValue())
                            :
                            String.format("remote+http://%s:%d", entry.getKey(), entry.getValue())
            );
            if (cnt < nodes.size()) sb.append(", ");
            cnt++;
            LOGGER.info("Nodes - Key : " + entry.getKey() + " Value : " + entry.getValue());
        }
        return sb.toString();
    }

    private InitialContext getInitialContext(Map<String, Integer> nodes, String remoteUser, String remotePassword, Path clientTrustStore, String keyStorePassword, String trustStoreType) throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException, CertificateException, IOException, NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.wildfly.naming.client.WildFlyInitialContextFactory");
        // For clusters see: https://access.redhat.com/solutions/721883 https://access.redhat.com/solutions/520183 e.g. p.put(Context.PROVIDER_URL, "host1:1100, host2:1100");
        properties.put(Context.PROVIDER_URL,
                formatProviderUrl(nodes, clientTrustStore != null));
        LOGGER.info("{}: {}", Context.PROVIDER_URL, properties.get(Context.PROVIDER_URL));
        LOGGER.info(properties.get(Context.PROVIDER_URL).toString());
        if (remoteUser != null) {
            properties.put(Context.SECURITY_PRINCIPAL, remoteUser);
            LOGGER.info("{}: {}", Context.SECURITY_PRINCIPAL, properties.get(Context.SECURITY_PRINCIPAL));
        }
        if (remotePassword != null) {
            properties.put(Context.SECURITY_CREDENTIALS, remotePassword);
            LOGGER.info("{}: {}", Context.SECURITY_CREDENTIALS, properties.get(Context.SECURITY_CREDENTIALS));
        }
        // TODO:
        // - get /opt/rh/eap7/root/usr/share/wildfly/standalone/configuration/keystore.jks
        // - export certificates: keytool -export -alias localhost -keystore keystore.jks -rfc -file server.crt -storepass redhat
        // - import certificate into client truststore: keytool -import -file server.crt -keystore client.ts -storepass redhat -noprompt
        // - javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword
        if (clientTrustStore != null) {
            if (!clientTrustStore.toFile().exists()) {
                throw new IllegalArgumentException(String.format("Client trust store %s doesn't exist!", clientTrustStore.toFile().getAbsolutePath()));
            }
            // https://maven.apache.org/guides/mini/guide-repository-ssl.html
            // https://docs.oracle.com/javadb/10.8.3.0/adminguide/cadminsslclient.html
            String previousVal = System.setProperty("javax.net.ssl.trustStore", clientTrustStore.toFile().getAbsolutePath());
            LOGGER.info("javax.net.ssl.trustStore={} (previous value: {})", System.getProperty("javax.net.ssl.trustStore"), previousVal);
            System.setProperty("javax.net.ssl.trustStorePassword", "redhat");
            System.setProperty("javax.net.ssl.trustStoreType", trustStoreType);
            LOGGER.info("javax.net.ssl.trustStore=" + System.getProperty("javax.net.ssl.trustStore"));
            // needed only when used with some other framework like Sunstore which messes with the SSLContext
            LOGGER.info("clientTrustStore=" + clientTrustStore.toFile().getAbsolutePath());
            // https://stackoverflow.com/questions/1793979/registering-multiple-keystores-in-jvm
            //configureTrustStore(clientTrustStore, keyStorePassword, trustStoreType);
        } else {
            LOGGER.info("No trustStore");
        }
        return new InitialContext(properties);
    }
}
