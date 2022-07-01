package b19021418;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class T2 {
    private static final String fileInput = "hdfs://172.16.17.10:9000/data/B19021418/stu.csv";
    private static final String fileOutput = "hdfs://172.16.17.10:9000/data/B19021418/output/T2";

    private static class WcMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) {
            try {
                //将文件的每一行传递过来，使用split分割后利用字符数组进行接收
                String[] stu = v1.toString().split(",");
                //向Reducer传递参数-> Key：学生姓名 Value：成绩
                context.write(new Text(stu[1]),new Text(stu[2]));
            } catch (Exception e) {
                System.out.println(v1.toString());
                e.printStackTrace();
            }
        }
    }

    private static class WcReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) {
            System.out.println("*********************************************************************");
            //定义一个ArrayList集合接收该学生的各项成绩
            List<Integer> scores = new ArrayList<>();
            for(Text value:values){
                scores.add(Integer.valueOf(value.toString()));
            }
            //对该学生的成绩进行求总分、平均分
            int num = 0, sum = 0;
            for(Integer score:scores){
                sum = sum + score.intValue();
                num = num + 1;
            }
            float avg = sum / num;
            //成绩排序
            Collections.sort(scores);
            //使用一个字符串拼接排好序的所有成绩
            String sort = "的总分:"+sum+" 平均分:"+avg+" 该生的成绩从低到高排序是:";
            for(Integer score:scores){
                sort = sort + score + "  ";
            }
            System.out.println(key.toString()+sort);
            //输出
            try {
                context.write(key,new Text(sort));
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
