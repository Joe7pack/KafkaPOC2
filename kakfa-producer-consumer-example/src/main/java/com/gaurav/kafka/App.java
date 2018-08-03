package com.gaurav.kafka;

import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.consumer.ConsumerCreator;
import com.gaurav.kafka.producer.ProducerCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class App {

	public static void main(String[] args) {

		if (args != null && args.length > 0 ) {
			if (args[0].equalsIgnoreCase("Producer")) {
				runProducer();
			} else if (args[0].equalsIgnoreCase("Consumer")){
				runConsumer();
		    }
		    else {
				System.out.println("must specify either Producer or Consumer on command line, exiting.");
				return;
			}
		}
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				consumer.close();
				return;
			}

			consumerRecords.forEach(record -> {
				loadJSONData(record.value());
			});
			consumer.commitAsync();
		}
		//consumer.close();
	}


	static String readStringFromURL(String requestURL) throws Exception
	{
		try (Scanner scanner = new Scanner(new URL(requestURL).openStream(),
				StandardCharsets.UTF_8.toString())) {
			scanner.useDelimiter("\\A");
			return scanner.hasNext() ? scanner.next() : "";
		}
	}

	static void loadJSONData(String jsonData) {
		try {
			Gson gson = new GsonBuilder().create();
			Type collectionType = new TypeToken<List<StockChart>>(){}.getType();
			List<StockChart> stockChartList = new Gson().fromJson(jsonData, collectionType);

			stockChartList.forEach(sc-> {
				String outputLine = String.format("Date: %1$s, Open: %2$s, High:  %3$s, Low: %4$s, Close: %5$s, Volume: %6$s, " +
								"Unadjusted Volume: %7$s, Change: %8$s, Change Percent: %9$s, Vwap: %10$s, Label: %11$s, " +
								"Change Over Time: %12$s", sc.date, sc.open,
						sc.high, sc.low, sc.close, sc.volume, sc.unadjustedVolume, sc.change, sc.changePercent, sc.vwap, sc.label, sc.changeOverTime);
				System.out.println("values: "+outputLine);
			});

		} catch (Exception e) {
			System.out.println("Error reading JSON data into StockList class: " + e);
		}
	}

	static void runProducer() {

		String dataFromURL = null;

		try {
			dataFromURL = readStringFromURL("https://api.iextrading.com/1.0/stock/aapl/chart");
		} catch (Exception e) {
			System.out.println("Error in reading URL: ");
			System.out.println(e);
			return;
		}

		Producer<Long, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, dataFromURL);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
				    + " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}

	public class StockChart {
		String date;
		String open;
		String high;
		String low;
		String close;
		String volume;
		String unadjustedVolume;
		String change;
		String changePercent;
		String vwap;
		String label;
		String changeOverTime;
	}

}
