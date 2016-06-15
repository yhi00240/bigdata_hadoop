package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool 
{
    public static void main(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));
        
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
     
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());               // 맵리듀스를 실행하기 위한 job 객체 생성
  
        job.setJarByClass(WordCount.class);                 // job 객체의 라이브러리를 지정
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);                  // reducer 클래스의 출력데이터와 key, value의 타입 설정
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);     // input과 output에 대한 지정
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
     
        return 0;    
    }
   
    public static class Map extends Mapper<LongWritable, Text, Text, Text> 
    {
        private Text key_word = new Text();
        private Text value_word = new Text();
      
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String tmpStr = value.toString();
            String equipment = "";
            String location = "";
            
            int textCheck = 0;
            tmpStr = tmpStr.replaceAll("[^0-9A-Za-z-.()]"," ");
            
            String[] token = tmpStr.split("\\s+"); /*공백 기준 단어 들어 {        ,source,  twitter,   for,    iPhone,   text, ...., locate, ...}
                                                                   token[0] token[1] token[2] token[3] token[4] token[5]     token[k]
                                                                                                                                   ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ 
                                                        source 다음 토큰에는  기기 종류가 들어간다. 그 후 text 토큰 ~locate 토큰 전까지 필요없는 토큰으로 분류하고,
                                                        locate 토큰 후부터 위치 정보가 들어간다.  */
            for(int i = 0; i < token.length; i++) 
            {
                if(token[i].equals("text"))                                         // 처음, text 토큰이 나올 때까지 반복
                {
                    for(int j = 2; j < i; j++)                                      // j=0은 token[0]=""(공백), token[1]="source", token[2]~token[i-1]까지 기기 종류에 대한 정보가 나옴
                    {
                        if(token[j].equals("Twitter") || token[j].equals("for"))    // Twitter for iPhone에서 Twitter for는 출력하지 않게 하기 위한 조건
                            continue;
                               
                        equipment = equipment.concat(token[j]);                     // Tweetbot Mac이라는 기기 종류가 나오면, 둘의 토큰 단어를 합쳐주기 위해 concat을 함
                        equipment = equipment.concat(" ");
                    }
                        textCheck = i; 
                        value_word.set(equipment);
                         
                        break;                                                      // 처음 text라는 단어가 나오고, 그 뒤에 text단어가 또 나올 수 있기 때문에 중복처리, 시간의 소모를 방지하기 위해 break
                  }
            }

            for(int i = token.length - 1; i > textCheck; i--)         // locate 토큰을 찾기위해 마지막 토큰부터 반복한다.
            {
                if(token[i].equals("locate"))
                {
                    for(int j = i + 1; j < token.length; j++)    // locate 토큰을 찾으면 +1하여 다음 토큰부터 concat한다. (위 기기 종류와 같은 방식)
                    {
                        location=location.concat(token[j]);
                        location=location.concat(" ");
                    }
                    
                    key_word.set(location);
                    
                    break;
                  }
            }       // data set에서 위치 정보 2가지를 활용했지만, 확장성을 위해 if(New york)이면 location에 New york을 넣는 것이 아닌, 위치 정보가 충분히 많다는 가정에서 생각함
           context.write(key_word, value_word);
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text>   //입력키 타입, 입력값 타입, 출력키 타입, 출력값 타입
   {    
        //@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {       
            HashMap<String,Integer> hm = new HashMap<String, Integer>();
            String ans = "";
                
            if(key.toString().equals("New York ")) 
                ans = "\tDevice        device_num\n\t\t\t\t\t";  
            else 
                ans = "Device        device_num\n\t\t\t\t\t";
                
            // map의 결과물로 locate(New york)={iPhone, Android, Android, iPad, iPad...}의 형식으로 받아온다.
            
            for(Text val : values)                                  // 각각의 기기가 몇개씩 존재하는 지 확인하기 위해 HashMap을 선언하여 파악한다.
            {
                String equ_suj = val.toString();
                Integer equ_num = hm.get(equ_suj);
                       
                if(equ_num != null)                             // 기기의 종류가 HashMap에 들어있으면 equ_num값에 null값이 아닌 현재 카운트된 수가 들어 있으므로 +1하여 다시 넣어준다.
                    hm.put(equ_suj, ++equ_num);  
                else
                    hm.put(equ_suj, 1);                             // 기기의 종류가 HashMap에 들어있지않으면 equ_num값에 null값이 들어가있으므로 1을 넣어준다.
            }
                
            Iterator it = sortByValue(hm).iterator(); // 기기의 종류가 많은 순으로 출력하기 위해 sort를 한다.
            
            while(it.hasNext())
            {
                String tmpStr=(String)it.next();
                Integer tmpNum=hm.get(tmpStr);
                       
                // 각각 나라 길이의 간격때문에 존재 하는 부분으로, 필요시 변경할 수 있다.
                if(key.toString().equals("New York ")) 
                    ans = ans + tmpStr + "\t\t\t" + tmpNum.toString() + "\n\t\t\t\t\t";
                else 
                    ans = ans + tmpStr + "\t\t\t" + tmpNum.toString() + "\n\t\t\t\t\t";
            }
                
            context.write(key,new Text(ans));   // 출력형식은 위치 정보(ex. New york)을 처음 한번만 출력하기 위해 key에 위치 정보가 들어가고,
                                                // value는 string형식으로 묶어서 Text형식으로 변환한다.
        }
    }
    
    public static List sortByValue(final HashMap map) // sort해주는 부분이다.
    {
        List<String> list = new ArrayList();
        
        list.addAll(map.keySet());
        
        Collections.sort(list,new Comparator(){   
            public int compare(Object o1,Object o2)
            {
                Object v1 = map.get(o1);
                Object v2 = map.get(o2);
               
                return ((Comparable) v1).compareTo(v2);
            }   
        });
    
        Collections.reverse(list);
        return list;
    }
}

