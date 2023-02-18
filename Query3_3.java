import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3_3 {
    public static class InMapper extends Mapper<Object, Text, Text, FloatWritable> {
        HashMap<Integer, String> custMap = new HashMap<Integer, String>();


        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            String record;
            int age;
            String ageRange = null;
            String custVal;

            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                while ((record = reader.readLine()) != null) {
                        String[] row = record.split(",");
                        age = Integer.parseInt(row[2]);
                        if (age >= 10 && age < 20) {
                            ageRange = "10-20";}
                        else if (age >= 20 && age < 30) {
                            ageRange = "20-30";}
                        else if (age >= 30 && age < 40) {
                            ageRange = "30-40";}
                        else if (age >= 40 && age < 50) {
                            ageRange = "40-50";}
                        else if (age >= 50 && age < 60) {
                            ageRange = "50-60";}
                        else if (age >= 60 && age <= 70) {
                            ageRange = "60-70";}

                        custVal = ageRange + ", " + row[3];

                        custMap.put(Integer.valueOf(row[0]), custVal);}
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            int custID = Integer.parseInt(row[1]);
            float transTotal = Float.parseFloat(row[2]);
            String mapKey = custMap.get(custID);

            context.write(new Text(mapKey), new FloatWritable(transTotal));
        }
    }

    public static class OpReducer extends Reducer<Text, FloatWritable, Text, Text>{
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
            int numCusts = 0;
            float minTransTotal = 1000;
            float maxTransTotal = 10;
            float totalTransTotal = 0;

            for (FloatWritable v : values) {
                numCusts++;
                totalTransTotal += v.get();

                if (v.get() < minTransTotal) {
                    minTransTotal = v.get();}

                if (v.get() > maxTransTotal) {
                    maxTransTotal = v.get();}
            }
            float avgTransTotal = totalTransTotal / numCusts;

            context.write(new Text(key), new Text(minTransTotal + ", " + maxTransTotal +", " + avgTransTotal));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Query3_3.class);
        job.setJobName("Query 3_3");

        job.addCacheFile(new URI("hdfs://localhost:9000/user/Project1/data/CustomersData.csv"));

        Path transInputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, transInputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(InMapper.class);
        job.setReducerClass(OpReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }
}