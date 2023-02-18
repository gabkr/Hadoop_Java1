import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3_2 {
    public static class InMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
        HashMap<Integer, Integer> custMap = new HashMap<Integer, Integer>();


        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            String record;

            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                while ((record = reader.readLine()) != null) {
                    String[] row = record.split(",");
                    custMap.put(Integer.valueOf(row[0]), Integer.valueOf(row[4]));}
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            int custID = Integer.parseInt(row[1]);
            int countryCode = Integer.parseInt(custMap.get(custID).toString());
            float transTotal = Float.parseFloat(row[2]);

            context.write(new IntWritable(countryCode), new FloatWritable(transTotal));
        }
    }

    public static class OpReducer extends Reducer<IntWritable, FloatWritable, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
            int numCusts = 0;
            float minTransTotal = 1000;
            float maxTransTotal = 10;

            for (FloatWritable v : values) {
                numCusts++;

                if (v.get() < minTransTotal) {
                    minTransTotal = v.get();}

                if (v.get() > maxTransTotal) {
                    maxTransTotal = v.get();}
            }
            context.write(key, new Text(numCusts + ", " + minTransTotal + ", " + maxTransTotal));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Query3_2.class);
        job.setJobName("Query 3_2");

		job.addCacheFile(new URI("hdfs://localhost:9000/user/Project1/data/CustomersData.csv"));

        Path transInputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, transInputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(InMapper.class);
        job.setReducerClass(OpReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }
}