import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Itcast .
 *
 * 这里就是mapreduce程序  mapper阶段业务逻辑实现的类
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *
 * KEYIN：表示mapper数据输入的时候key的数据类型，在默认的读取数据组件下，叫InputFormat,它的行为是一行一行的读取待处理的数据
 *        读取一行，返回一行给我们的mr程序，这种情况下  keyin就表示每一行的起始偏移量  因此数据类型是Long
 *
 * VALUEIN:表述mapper数据输入的时候value的数据类型，在默认的读取数据组件下 valuein就表示读取的这一行内容  因此数据类型是String
 *
 * KEYOUT 表示mapper数据输出的时候key的数据类型  在本案例当中 输出的key是单词  因此数据类型是 String
 *
 * VALUEOUT表示mapper数据输出的时候value的数据类型  在本案例当中 输出的key是单词的次数  因此数据类型是 Integer
 *
 * 这里所说的数据类型String Long都是jdk自带的类型   在序列化的时候  效率低下 因此hadoop自己封装一套数据类型
 *   long---->LongWritable
 *   String-->Text
 *   Integer--->Intwritable
 *   null-->NullWritable
 *
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    /**
     *  这里就是mapper阶段具体的业务逻辑实现方法  该方法的调用取决于读取数据的组件有没有给mr传入数据
     *  如果有的话  每传入一个《k,v》对  该方法就会被调用一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        //拿到传入进来的一行内容，把数据类型转化为String 
        String line = value.toString();

        //将这一行内容按照分隔符进行一行内容的切割 切割成一个单词数组
        String[] words = line.split(" ");

        //遍历数组，每出现一个单词  就标记一个数字1  <单词，1>
        for (String word : words) {
            //使用mr程序的上下文context 把mapper阶段处理的数据发送出去
            //作为reduce节点的输入数据
            context.write(new Text(word),new IntWritable(1));
            //hadoop hadoop spark -->   <hadoop,1><hadoop,1><spark,1>
        }
    }
}
