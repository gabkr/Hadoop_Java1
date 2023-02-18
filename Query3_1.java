import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3_1 {

    public static class CustomerMapper extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String custRecord = value.toString();
            String[] custValues = custRecord.split(",");
            int custID = Integer.parseInt(custValues[0]);
            String result = "Cust:" + custValues[1] + "," + custValues[5];
            context.write(new IntWritable(custID), new Text(result));
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String transRecord = value.toString();
            String[] transValues = transRecord.split(",");
            int t_custID = Integer.parseInt(transValues[1]);
            String result = "Trans:" + transValues[2] + "," + transValues[3];
            context.write(new IntWritable(t_custID), new Text(result));
        }
    }


    public static class JoinReducer extends Reducer<IntWritable,Text,IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            String salary = null;
            int numTrans = 0;
            float totalSum = 0;
            int minItems = 10;

            for (Text v : values) {
                String[] vSplit = v.toString().split(":");
                if (vSplit[0].equalsIgnoreCase("Cust")) {
                    String[] cSplit = vSplit[1].split(",");
                    name = cSplit[0];
                    salary = cSplit[1];}
                else if (vSplit[0].equalsIgnoreCase("Trans")) {
                    String[] tSplit = vSplit[1].split(",");
                    numTrans++;
                    totalSum += Float.parseFloat(tSplit[0]);
                    int numItems = Integer.parseInt(tSplit[1]);
                    if (numItems < minItems) {
                        minItems = numItems;}}
            }

            context.write(key, new Text(name + "," + salary + "," + String.valueOf(numTrans) + "," + String.valueOf(totalSum) + "," + String.valueOf(minItems)));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Query3_1.class);
        job.setJobName("Query3_1");


        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        Path customerInputPath = new Path(args[0]);
        Path transactionInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        MultipleInputs.addInputPath(job, customerInputPath, TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, transactionInputPath, TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}