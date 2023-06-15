package dlr.ts.reallabhh.diakLC.FlinkKafkaConsumerLC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.client.Connection;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

public class DiakLC {

	public static void main(String[] args) throws Exception {

		// fetch runtime arguments
		String bootstrapServers = "your bootstrap server address";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the Consumer and create a datastream from this source
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty("group.id", "diak_lc");
		final FlinkKafkaConsumer<String> flinkConsumer = new FlinkKafkaConsumer<>("diak_lc", // input topic
				new SimpleStringSchema(), // serialization schema
				properties); // properties

		flinkConsumer.setStartFromTimestamp(Long.parseLong("0"));

		DataStream<String> readingStream = env.addSource(flinkConsumer);
		readingStream.rebalance().map(new RichMapFunction<String, String>() {

			private static final long serialVersionUID = -2547861355L; // random number
			

			//batch insert based on time difference
			int maxTimeDiff = 2000; // 2000 milliseconds = 2 seconds 
			long lastUpdatedTime;
			
			// Define all the global variables here which will be used in this application
			// variables to store Kafka msg processing summary
			lastUpdatedTime = Instant.now().toEpochMilli();
			private long numberOfMessagesProcessed;
			private long numberOfMessagesFailed;
			private long numberOfMessagesSkipped;


			// Define the variables for both the Live and History tables in which the data
			// will be store in the HBase
			DataStore lc_live = null;
			DataStore lc_history = null;

			SimpleFeatureType sft_live;
			SimpleFeatureType sft_history;
			SimpleFeatureBuilder SFbuilderHist; // feature builder for history
			SimpleFeatureBuilder SFbuilderLive; // feature builder for live

			List<SimpleFeature> lc_history_features; // Features list of SimpleFeature-s to store diak:lc_history
														// messages
			List<SimpleFeature> lc_live_features; // Features list of SimpleFeature-s to store diak:lc_live messages
			
			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("In open method.");

				// --- GEOMESA, GEOTOOLS APPROACH ---//
				// define connection parameters to diak:lc_live GeoMesa-HBase DataStore
				Map<String, Serializable> params_live = new HashMap<>();
				params_live.put("hbase.catalog", "diak:lc_live"); // HBase table name
				params_live.put("hbase.zookeepers",
						"ts-bd-hadoop-hbase-01-ba.intra.dlr.de:2181,ts-bd-hadoop-hbase-02-ba.intra.dlr.de:2181");

				try {
					lc_live = DataStoreFinder.getDataStore(params_live);
					if (lc_live == null) {
						System.out.println("Could not connect to diak:lc_live");
					} else {
						System.out.println("Successfully connected to diak:lc_live");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				// create simple feature type for diak:lc_live_table in HBASE which will have
				// the following columns elements in the table
				StringBuilder attributes = new StringBuilder();
				attributes.append("timestamp:Long,");
				attributes.append("source:String,");
				attributes.append("lctype:String,");
				attributes.append("lcRange:Double,");
				attributes.append("status:String,");
				attributes.append("forecast:Double,");
				attributes.append("carsCount:Integer,");
				attributes.append("*lcPosition:Point:srid=4326");
				// a table template will be created below with the name, "sft_live", having all
				// the above columns
				sft_live = SimpleFeatureTypes.createType("diak_lc_live", attributes.toString());

				// define connection parameters to diak:lc_history GeoMesa-HBase DataStore
				Map<String, Serializable> params_his = new HashMap<>();
				params_his.put("hbase.catalog", "diak:lc_history"); // HBase table name
				params_his.put("hbase.zookeepers",
						"ts-bd-bs,ts-bd-bs"); // hbase.zookeepers address (dummy here)

				try {
					lc_history = DataStoreFinder.getDataStore(params_his);
					if (lc_history == null) {
						System.out.println("Could not connect to diak:lc_history");
					} else {
						System.out.println("Successfully connected to diak:lc_history");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				// create simple feature type for diak_lc_history_table in HBASE which will have
				// the following columns elements in the table
				StringBuilder attributes1 = new StringBuilder();
				attributes1.append("lcID:String,");
				attributes1.append("timestamp:Long,");
				attributes1.append("source:String,");
				attributes1.append("lctype:String,");
				attributes1.append("lcRange:Double,");
				attributes1.append("status:String,");
				attributes1.append("forecast:Double,");
				attributes1.append("carsCount:Integer,");
				attributes1.append("*lcPosition:Point:srid=4326");
				// a table template will be created below with the name, "sft_history", having
				// all the above columns
				sft_history = SimpleFeatureTypes.createType("diak_lc_history", attributes1.toString());

				try {
					lc_history.createSchema(sft_history); // created if it doesn't exist. If not, execution of this
															// statement does nothing

					lc_live.createSchema(sft_live); // created if it doesn't exist. If not, execution of this statement
													// does nothing

				} catch (IOException e) {
					e.printStackTrace();
				}

				// Initialize the variables
				numberOfMessagesProcessed = 0;
				numberOfMessagesFailed = 0;
				numberOfMessagesSkipped = 0;

				// for lc_Live
				lc_live_features = new ArrayList<>();
				SFbuilderLive = new SimpleFeatureBuilder(sft_live);

				// for lc_History
				lc_history_features = new ArrayList<>();
				SFbuilderHist = new SimpleFeatureBuilder(sft_history);
			}

			public String map(String valueFromKafka) throws Exception {
				// this function runs for every message from kafka topic							    
							 System.out.println(flinkConsumer.toString() + " Executing batch...");
												
							try {				
							//diak:lc_live GeoMesa-HBase DataStore
							//copy the list into a local variable and empty the list for the next iteration
							List<SimpleFeature> LocalFeatures = lc_live_features;
							lc_live_features = new ArrayList<>();
							LocalFeatures = Collections.unmodifiableList(LocalFeatures);
							try (FeatureWriter<SimpleFeatureType,SimpleFeature> writer = 
							lc_live.getFeatureWriterAppend(sft_live.getTypeName(), Transaction.AUTO_COMMIT)){
								System.out.println("Writing "+LocalFeatures.size()+" features to diak:lc_live");
								for (SimpleFeature feature : LocalFeatures) {
									SimpleFeature toWrite = writer.next();
									toWrite.setAttributes(feature.getAttributes());
									((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
									toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
									toWrite.getUserData().putAll(feature.getUserData());
									writer.write();
								}										
							} catch (IOException e) {
								e.printStackTrace();
							}
						
				
							//diak:lc_history GeoMesa-HBase DataStore
							//copy the list into a local variable and empty the list for the next iteration
							LocalFeatures = lc_history_features;
							lc_history_features = new ArrayList<>();
							LocalFeatures = Collections.unmodifiableList(LocalFeatures);
							try (FeatureWriter<SimpleFeatureType,SimpleFeature> writer = 
							lc_live.getFeatureWriterAppend(sft_history.getTypeName(), Transaction.AUTO_COMMIT)){
								System.out.println("Writing "+LocalFeatures.size()+" features to diak:lc_history");
								for (SimpleFeature feature : LocalFeatures) {
									SimpleFeature toWrite = writer.next();
									toWrite.setAttributes(feature.getAttributes());
									((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
									toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
									toWrite.getUserData().putAll(feature.getUserData());
									writer.write();
								}										
							} catch (IOException e) {
								e.printStackTrace();
							}
							
							lastUpdatedTime = Instant.now().toEpochMilli();
						}
						catch (IOException e) {
							System.out.println("Error executing batch : "+e);
						}
						TimeUnit.SECONDS.sleep(4);
					}
				}
				System.out.println("Looping stopped as HBase Connection has been closed.");
			} catch (Exception e) {
				e.printStackTrace();
			} 
		});
	}
				
				//Create a new thread to execute batches every n seconds //The loop would run until the HBase connection is open
				CompletableFuture.runAsync(()->{
					try {
						
						// tumbling event-time windows
						readingStream.keyBy(0)
						    .window(TumblingEventTimeWindows.of(Time.seconds(2)))
						    
						 System.out.println(flinkConsumer.toString() + " Executing batch...");
						
						//diak:lc_live GeoMesa-HBase DataStore
						//copy the list into a local variable and empty the list for the next iteration
						List<SimpleFeature> LocalFeatures = lc_live_features;
						lc_live_features = new ArrayList<>();
						LocalFeatures = Collections.unmodifiableList(LocalFeatures);
						try (FeatureWriter<SimpleFeatureType,SimpleFeature> writer = lc_live.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)){
							System.out.println("Writing "+LocalFeatures.size()+" features to diak:lc_live");
							for (SimpleFeature feature : LocalFeatures) {
								SimpleFeature toWrite = writer.next();
								toWrite.setAttributes(feature.getAttributes());
								((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
								toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
								toWrite.getUserData().putAll(feature.getUserData());
								writer.write();
							}										
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
			}

				// check if the message has any content
				if (valueFromKafka == null || valueFromKafka.trim().isEmpty()) {
					System.out.println("Message from Kafka is empty!");
					numberOfMessagesSkipped++;
					return "Warning: Message from Kafka is empty";
				} else {
					try {
											
						// value from Kafka json = it will contain the status of LC, no of cars at LC
						// from Geo-server = will contain properies of LC such as type of LC wether it
						// has one barrier or double etc

						JSONObject json_obj = new JSONObject(valueFromKafka);
						//System.out.println(json_obj.toString()); //shan_sa

						String lcIDhistory = json_obj.getString("id"); //shan_sa
						String[] splitObj = lcIDhistory.split("-"); // split the feature of interest as we have 3
																	// features there

						String lcID_history = splitObj[0] + "-" + splitObj[1]; // for history table rowID index 0 + index 1 i.e. (lcID
																	// + timestamp) b/c it should be unique
						
						String lcID = splitObj[0];
						// for live table lcID will be just index 0 i.e. lcID with no timestamp

						String sourceID = splitObj[2]; // if source ID = 0, field else its simulation.

						String source = sourceID;
						if (source == "0") {
							source = "FIELD";
						} else {
							source = "SIMULATION";
						}

						String timestamp = json_obj.getJSONObject("phenomenonTime").getString("instant"); //shan_sa: use phenomenonTime as it contains the timestamp at which the observation was made
						
						// connect to the URL json in order to get static values such as lctype and lcPosition
						URL url = new URL(json_obj.getString("featureOfInterest")); //shan_sa: URL can be directly fetched from kafka message 

						// Type cast the URL object into a HttpURLConnection object to apply the
						// properties of the HttpURLConnection class to validate features
						HttpURLConnection conn = (HttpURLConnection) url.openConnection();

						// Set the request type, as the request to a GET request
						conn.setRequestMethod("GET");
						conn.setConnectTimeout(5000);
						conn.setReadTimeout(5000);
						conn.setDoOutput(true); //shan_sa
						String username = "rs-pdiak_reader";
						String password = "2VWkstLqNdIkkXsZaBZD";

						// Open a connection stream to the corresponding API
						String userpass = username + ":" + password;
						String basicAuth = Base64.getEncoder().encodeToString(userpass.getBytes(StandardCharsets.UTF_8));
						conn.setRequestProperty("Authorization", "Basic "+basicAuth); 
						
						InputStream content = (InputStream) conn.getInputStream(); //getInputStream method is to be used to get the response from the server

						int responsecode = conn.getResponseCode();
						String urlinline = ""; //shan_sa: initializing the string as null results in the text "null" being present at the beginning of the obj after execution of line 288
						if (responsecode != 200)
							throw new RuntimeException("HttpResponseCode: " + responsecode);
						else {
							
				            BufferedReader inBR = new BufferedReader (new InputStreamReader (content));
				            String line;
				            while ((line = inBR.readLine()) != null) {
				            	urlinline += line;
				            }
						}

						// JSONArray jsonArray = new JSONArray (urlinline);
						JSONArray valueList = (JSONArray) json_obj.getJSONArray("member");
						String status = valueList.getJSONObject(0).getJSONObject("result").getString("value").toUpperCase(); //shan_sa: status must be in uppercase
						
						//forecast is a timestamp (string)in the kafka message, which needs to be converted to double
						//the timestamp format varies slightly between field and simulation
						long forecast = -9999;
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						
						String forecast_str = valueList.getJSONObject(1).getJSONObject("result").getString("value");
						Date forecast_dt;
						forecast_dt = sdf.parse(forecast_str);
						forecast = forecast_dt.getTime() / 1000;
												
						int carsCount = valueList.getJSONObject(2).getJSONObject("result").getInt("value");
						Double lcRange = valueList.getJSONObject(3).getJSONObject("result").getDouble("value"); 
						
						// values from the GEO SERVER URL connection
						JSONObject json_objURL = new JSONObject(urlinline);

						JSONArray url_Array = (JSONArray) json_objURL.getJSONArray("features");
						
						//Skip message if the response from geoserver contains no features
						if(url_Array.length()==0) {
							System.out.println("WARNING: Response from GeoServer has no features! Skipping message.");
							numberOfMessagesSkipped++;
							return "Warning: Response from GeoServer has no features";
						}					

						String lctype = url_Array.getJSONObject(0).getJSONObject("properties").getString("EXTRAINFO");
						JSONArray coordinatesPosition = url_Array.getJSONObject(0).getJSONObject("geometry")
								.getJSONArray("coordinates");
						String lcPosition = "POINT(" + coordinatesPosition.getFloat(0) + " "
								+ coordinatesPosition.getFloat(1) + ")";

						Date dt;
						dt = sdf.parse(timestamp);
						long epoch = dt.getTime() / 1000;

						// for the History table lcID and timestamp = rowID
						// create and add feature to the lc_history SFbuilder
						SFbuilderHist.set("lcID", lcID);
						SFbuilderHist.set("lctype", lctype);
						SFbuilderHist.set("timestamp", epoch); 
						SFbuilderHist.set("source", source);
						SFbuilderHist.set("lcRange", lcRange);
						SFbuilderHist.set("status", status);
						SFbuilderHist.set("forecast", forecast);
						SFbuilderHist.set("carsCount", carsCount);
						SFbuilderHist.set("lcPosition", lcPosition);
						SFbuilderHist.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
						lc_history_features = new ArrayList<>();
						lc_history_features.add(SFbuilderHist.buildFeature(lcID_history));

						try (FeatureWriter<SimpleFeatureType, SimpleFeature> writerHistory = lc_history
								.getFeatureWriterAppend(sft_history.getTypeName(), Transaction.AUTO_COMMIT)) {
							System.out.println("Writing " + lc_history_features.size() + " features to diak:lc_history");
							for (SimpleFeature feature : lc_history_features) {
								SimpleFeature toWrite = writerHistory.next();
								toWrite.setAttributes(feature.getAttributes());
								((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
								toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
								toWrite.getUserData().putAll(feature.getUserData());
								writerHistory.write();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}

						// delete row if already present in table
						FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
						Filter filter = ff.id(Collections.singleton(ff.featureId(lcID)));
						SimpleFeatureStore featurestore_live = (SimpleFeatureStore) lc_live
								.getFeatureSource("diak_lc_live");
						featurestore_live.removeFeatures(filter);

						// create and add feature to the vehicle live SFbuilder
						SFbuilderLive.set("lctype", lctype);
						SFbuilderLive.set("timestamp", epoch);
						SFbuilderLive.set("lcRange", lcRange);
						SFbuilderLive.set("status", status);
						SFbuilderLive.set("source", source);
						SFbuilderLive.set("forecast", forecast);
						SFbuilderLive.set("carsCount", carsCount);
						SFbuilderLive.set("lcPosition", lcPosition);
						SFbuilderLive.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
						lc_live_features = new ArrayList<>();
						lc_live_features.add(SFbuilderLive.buildFeature(lcID));
						System.out.println(lcRange);

						try (FeatureWriter<SimpleFeatureType, SimpleFeature> writerLive = lc_live
								.getFeatureWriterAppend(sft_live.getTypeName(), Transaction.AUTO_COMMIT)) {
							System.out.println("Writing " + lc_live_features.size() + " features to diak:lc_live");
							for (SimpleFeature feature : lc_live_features) {
								SimpleFeature toWrite = writerLive.next();
								toWrite.setAttributes(feature.getAttributes());
								((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
								toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
								toWrite.getUserData().putAll(feature.getUserData());
								writerLive.write();
							}
							numberOfMessagesProcessed++; //shan_sa
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(valueFromKafka); //shan_sa
						numberOfMessagesFailed++; //shan_sa
					}
				}
				return "success";
			}

			@Override
			public void close() throws Exception {
				// this function runs when the flink job stops/is stopped

				// close datastore and connection
				if (lc_live != null)
					lc_live.dispose();
				if (lc_history != null)
					lc_history.dispose();
				//connection.close(); //shan_sa: connection object is unused
				System.out.println("Connections to HBase Datastore successfully closed");

				// Print processing stats
				System.out.println(flinkConsumer.toString() + " Number of messages successfully processed: "
						+ numberOfMessagesProcessed);
				System.out.println(flinkConsumer.toString() + " Number of messages failed: " + numberOfMessagesFailed);
				System.out
						.println(flinkConsumer.toString() + " Number of messages skipped: " + numberOfMessagesSkipped);

			}

		});

		env.execute("dlr.ts.reallabhh.diakLC.FlinkKafkaConsumerLC");
	}
}