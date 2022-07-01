package b19021418;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class T1 {
    private static final String fileInput = "hdfs://172.16.17.10:9000/data/B19021418/stu.csv";
    private static final String fileOutput = "hdfs://172.16.17.10:9000/data/B19021418/output/T1";

    private static class WcMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) {
            try {
                //将文件的每一行传递过来，使用split分割后利用字符数组进行接收
                String[] splits = v1.toString().split(",");

                //向Reducer传递参数-> Key：课程 Value：成绩
                context.write(new Text(splits[0]), new Text(splits[2]));
            } catch (Exception e) {
                System.out.println(v1.toString());
                e.printStackTrace();
            }
        }
    }

    private static class WcReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) {
            //Arraylist集合储存所有的成绩数据，借用collections的方法求最大值最小值
            List<Integer> list = new ArrayList<>();
            for (Text v : values) {
                list.add(Integer.valueOf(v.toString()));
            }
            //求max及min
            int maxScore = Collections.max(list);
            int minScore = Collections.min(list);
            // 求平均成绩
            int sum = 0;
            for (int score : list) {
                sum += score;
            }
            double avg = sum / list.size();
            System.out.println("*****************************************");
            String result = "的最高分:" + maxScore + "    最低分:" + minScore + "    平均分:" + avg;
            System.out.println(key.toString() + result);
            try {
                context.write(key, new Text(result));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Worker {
        public void deal() {
            //设置文件输入路径
            Path fileInPath = new Path(fileInput);
            //设置文件输出路径
            Path fileOutPath = new Path(fileOutput);
            //进行配置
            Configuration conf = new Configuration();
            try {
                Job job = Job.getInstance(conf);
                job.setJarByClass(Worker.class);
                job.setMapperClass(WcMapper.class);
                job.setReducerClass(WcReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                //设置文件路径
                FileInputFormat.addInputPath(job, fileInPath);
                FileOutputFormat.setOutputPath(job, fileOutPath);
                job.waitForCompletion(true);
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.deal();
    }
}
