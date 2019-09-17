/*
 * Copyright © 2014-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.appender.kafka;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.LoggingConfiguration;
import com.google.common.util.concurrent.Futures;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.twill.common.Threads;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Kafka producer that publishes log messages to Kafka brokers.
 */
final class SimpleKafkaProducer {

  // Kafka producer is thread safe
  private final KafkaProducer<String, byte[]> producer;

  SimpleKafkaProducer(CConfiguration cConf) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cConf.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
    try {
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
    } catch (Exception ex) {
      System.out.println("class not found exception : " + ex);
    }
    //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    //props.setProperty("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    props.setProperty("producer.type", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_TYPE,
                       LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_TYPE));
    props.setProperty("queue.buffering.max.ms", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_BUFFER_MS,
                      Long.toString(LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_BUFFER_MS)));
    props.setProperty(Constants.Logging.NUM_PARTITIONS, cConf.get(Constants.Logging.NUM_PARTITIONS));

    producer = createProducer(props);
  }

  void publish(List<ProducerRecord<String, byte[]>> messages) {
    // Clear the interrupt flag, otherwise it won't be able to publish
    boolean threadInterrupted = Thread.interrupted();
    try {
      Iterator<ProducerRecord<String, byte[]>> it = messages.iterator();
    	while (it.hasNext()) {
  	    ProducerRecord<String, byte[]> producerRecord = it.next();
            producer.send(producerRecord);
    	}
    } finally {
      // Reset the interrupt flag if needed
      if (threadInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  void stop() {
    producer.close();
  }

  /**
   * Creates a {@link Producer} using the given configuration. The producer instance will be created from a
   * daemon thread to make sure the async thread created inside Kafka is also a daemon thread.
   */
  private <K, V> KafkaProducer<K, V> createProducer(final Properties props) {
    ExecutorService executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("create-producer"));
    try {
      return Futures.getUnchecked(executor.submit(() -> new org.apache.kafka.clients.producer.KafkaProducer<>(props)));
    } finally {
      executor.shutdownNow();
    }
  }
}
