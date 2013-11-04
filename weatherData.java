/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
//package weatherdata;

/**
 *
 * @author Dipesh
 */
// 	import org.w3c.dom.Text;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;



import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class weatherData {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
// 	      private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Double temperature;
            Double humidity;
            Double rainfall;
            String MapOutPut;

            String[] words = line.split(",");
            if (words.length == 6) {
                String keycity = words[0] + ":" + words[1];
                temperature = Double.parseDouble(words[3]);
                humidity = Double.parseDouble(words[4]);
                rainfall = Double.parseDouble(words[5]);
                MapOutPut = Double.toString(temperature) + ";" + Double.toString(humidity) + ";" + Double.toString(rainfall);

//	 	      Text mapOutput = new Text();
                output.collect(new Text(keycity), new Text(MapOutPut));
            }
        }
    }

    public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int counter = 0;
            double temperatureMax, temperatureMin, Temp, humidityMax, humidityMin, Hum, rainfallTotal;
            String Stringvalue;

            temperatureMax = 0;
            temperatureMin = 0;
            humidityMax = 0;
            humidityMin = 0;
            rainfallTotal = 0;
            double tempSum = 0;
            double humSum = 0;
            String[] temp;
            int date;
            //String keyvalue=key.toString();

            //String delimiter = "-|;|,|\t";
            //temp = keyvalue.split(delimiter);
            //String cityname= temp[0];
            //date = Integer.valueOf(temp[1])*10000+Integer.valueOf(temp[2])*100+Integer.valueOf(temp[3]);	


            while (values.hasNext()) {
                Stringvalue = values.next().toString();
                counter++;
                temp = Stringvalue.split(";");
                Temp = Double.parseDouble(temp[0]);
                // Max Temperature
                if (temperatureMax == 0) {
                    temperatureMax = Temp;
                }
                if ((temperatureMax < Temp) && (temperatureMax != 0)) {
                    temperatureMax = Temp;
                }
                //Min Temperature
                if (temperatureMin == 0) {
                    temperatureMin = Temp;
                }
                if ((temperatureMin > Temp) && (temperatureMin != 0)) {
                    temperatureMin = Temp;
                }
                //Total Temperature
                tempSum += Temp;
                //Max Humidity
                Hum = Double.parseDouble(temp[1]);
                if (humidityMax == 0) {
                    humidityMax = Hum;
                }
                if ((humidityMax < Hum) && (humidityMax != 0)) {
                    humidityMax = Hum;
                }
                //Min Humidity
                if (humidityMin == 0) {
                    humidityMin = Hum;
                }
                if ((humidityMin > Hum) && (humidityMin != 0)) {
                    humidityMin = Hum;
                }
                //Total Humidity
                humSum += Hum;

                rainfallTotal += Double.parseDouble(temp[2]);
// 		         sum += values.next().get();
            }



            String data = String.format("%.2f", temperatureMax) + "," + String.format("%.2f", temperatureMin) + "," + String.format("%.2f", tempSum) + ","
                    + String.format("%.2f", humidityMax) + "," + String.format("%.2f", humidityMin) + "," + String.format("%.2f", humSum) + "," + String.format("%.2f", rainfallTotal) + "," + Integer.toString(counter);
            output.collect(key, new Text(data));

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private static BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAI4PYHEAKADYPN5TA", "MeMPKhaUFsk55dSkCdBo5X7xBdsLqQqsU8813J0Q");
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
        private static String tableName = "weatherdata";

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int counter = 0;
           
            int oldCounter = 0;
            double oldAveTemp = 0;
            double oldAveHum = 0;
            double temperatureMax, temperatureMin, Temp, humidityMax, humidityMin, Hum, rainfallTotal;
            String Stringvalue;


            String keyvalue = key.toString();
            String[] temp;
            String delimiter = "-|:|,|\t";
            temp = keyvalue.split(delimiter);
            temperatureMax = 0;
            temperatureMin = 0;
            humidityMax = 0;
            humidityMin = 0;
            rainfallTotal = 0;
            double tempSum = 0;
            double humSum = 0;

            int date;
            String cityname = temp[0];
            date = Integer.valueOf(temp[1]) * 10000 + Integer.valueOf(temp[2]) * 100 + Integer.valueOf(temp[3]);

            while (values.hasNext()) {
                Stringvalue = values.next().toString();
                //counter++;
                temp = Stringvalue.split(",");


                //Max Temperature
                Temp = Double.parseDouble(temp[0]);
                if (temperatureMax == 0) {
                    temperatureMax = Temp;
                }
                if ((temperatureMax < Temp) && (temperatureMax != 0)) {
                    temperatureMax = Temp;
                }

                //Min Temperature
                Temp = Double.parseDouble(temp[1]);
                if (temperatureMin == 0) {
                    temperatureMin = Temp;
                }
                if ((temperatureMin > Temp) && (temperatureMin != 0)) {
                    temperatureMin = Temp;
                }

                //Total temperature
                Temp = Double.parseDouble(temp[2]);
                tempSum += Temp;

                //Max Humidity
                Temp = Double.parseDouble(temp[3]);
                if (humidityMax == 0) {
                    humidityMax = Temp;
                }
                if ((humidityMax < Temp) && (humidityMax != 0)) {
                    humidityMax = Temp;
                }

                //Min Humidity
                Temp = Double.parseDouble(temp[4]);
                if (humidityMin == 0) {
                    humidityMin = Temp;
                }
                if ((humidityMin > Temp) && (humidityMin != 0)) {
                    humidityMin = Temp;
                }
                //Total Humidity
                humSum += Double.parseDouble(temp[5]);
                //Total Rainfall
                rainfallTotal += Double.parseDouble(temp[6]);

                //Summing counter
                counter += Integer.parseInt(temp[7]);
// 		         sum += values.next().get();
            }
            tempSum /= counter;
            humSum /= counter;
           
            //export to Dynamo
            client.setEndpoint("dynamodb.eu-west-1.amazonaws.com");
            if (counter != 24) {
//If data is not present for 24 hours in reducer then check for presence of existing data in DB
                Condition rangeKeyCondition = new Condition()
                        .withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue().withN(String.valueOf(date)));

                QueryRequest queryRequest = new QueryRequest()
                        .withTableName("weatherdata")
                        .withHashKeyValue(new AttributeValue().withS(cityname))
                        .withAttributesToGet(Arrays.asList("City", "Date", "MaxTemperature", "MinTemperature", "AverageTemperature", "MaxHumidity", "MinHumidity", "AverageHumidity", "TotalRainfall", "Counter"))
                        .withRangeKeyCondition(rangeKeyCondition);
                QueryResult result = client.query(queryRequest);

                if (result != null) {  //input existed 
                    for (java.util.Map<String, AttributeValue> resultitem : result.getItems()) {
                        for (java.util.Map.Entry<String, AttributeValue> readitem : resultitem.entrySet()) {
                            String attributeName = readitem.getKey();
                            AttributeValue value = readitem.getValue();
                            if (attributeName == "MaxTemperature") {
                                temperatureMax = Double.valueOf(value.getN()) > temperatureMax ? Double.valueOf(value.getN()) : temperatureMax;
                            } else if (attributeName == "MinTemperature") {
                                temperatureMin = Double.valueOf(value.getN()) < temperatureMin ? Double.valueOf(value.getN()) : temperatureMin;
                            } else if (attributeName == "MaxHumidity") {
                                humidityMax = Double.valueOf(value.getN()) > humidityMax ? Double.valueOf(value.getN()) : humidityMax;
                            } else if (attributeName == "MinHumidity") {
                                humidityMin = Double.valueOf(value.getN()) < humidityMin ? Double.valueOf(value.getN()) : humidityMin;
                            } else if (attributeName == "TotalRainfall") {
                                rainfallTotal = Double.valueOf(value.getN()) + rainfallTotal;
                            } else if (attributeName == "Counter") {
                                oldCounter = Integer.valueOf(value.getN());
                            } else if (attributeName == "AverageTemperature") {
                                oldAveTemp = Double.valueOf(value.getN());
                            } else if (attributeName == "AverageHumidity") {
                                oldAveHum = Double.valueOf(value.getN());
                            }
                        }
                    }
                    tempSum = ((counter * tempSum) + (oldCounter * oldAveTemp)) / (counter + oldCounter);
                    humSum = ((counter * humSum) + (oldCounter * oldAveHum)) / (counter + oldCounter);
                    counter = (counter + oldCounter);
                }
            }
            
            java.util.Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("City", new AttributeValue().withS(cityname));
            item.put("Date", new AttributeValue().withN(Integer.toString(date)));
            item.put("MaxTemperature", new AttributeValue().withN(Double.toString(temperatureMax)));
            item.put("MinTemperature", new AttributeValue().withN(Double.toString(temperatureMin)));
            item.put("AverageTemperature", new AttributeValue().withN(Double.toString(tempSum)));
            item.put("MaxHumidity", new AttributeValue().withN(Double.toString(humidityMax)));
            item.put("MinHumidity", new AttributeValue().withN(Double.toString(humidityMin)));
            item.put("AverageHumidity", new AttributeValue().withN(Double.toString(humSum)));
            item.put("TotalRainfall", new AttributeValue().withN(Double.toString(rainfallTotal)));
            item.put("Counter", new AttributeValue().withN(Integer.toString(counter)));
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(item);
            PutItemResult result = client.putItem(putItemRequest);
            String data = String.format("%.2f", temperatureMax) + "," + String.format("%.2f", temperatureMin) + "," + String.format("%.2f", tempSum) + ","
                    + String.format("%.2f", humidityMax) + "," + String.format("%.2f", humidityMin) + "," + String.format("%.2f", humSum) + "," + String.format("%.2f", rainfallTotal) + "," + Integer.toString(counter);
            output.collect(key, new Text(data));

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(weatherData.class);
        conf.setJobName("weather log");

// 		      conf.setMapOutputKeyClass(Text.class);
//		      conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combiner.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
