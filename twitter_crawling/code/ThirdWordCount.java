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
 
        Job job = Job.getInstance(getConf());   // 맵리듀스를 실행 하기 위한 job 객체 생성
 
        job.setJarByClass(WordCount.class);     // job 객체의 라이브러리를 지정
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
     
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        job.setOutputKeyClass(Text.class);      // 리듀서 클래스의 출력데이터와 key와 value의 타입을 설정한다.
        job.setOutputValueClass(Text.class);
 
        job.setInputFormatClass(TextInputFormat.class);     // input과 output에 대한 지정
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
   
        return 0;
    }
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text>  //line number, input(real data), output, output
    {
        private Text key_word = new Text();
        private Text value_word = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {                       
            String[] candidate = { "CLINTON", "SANDERS", "CRUZ", "KASICH","TRUMP" };                            //사용할 대선 후보 이름
            String[] twitid = { "HILLARYCLINTON", "BERNIESANDERS", "TEDCRUZ", "JOHNKASICH","REALDONALDTRUMP" }; //대선후보 twitter id
            String[] namechk = { "HILLARY", "BERNIE", "TED", "JOHN","DONALD" };                                //대선 후보 이름 앞부분
                                
            String tmpStr = value.toString();
            tmpStr = tmpStr.replaceAll("[^A-Za-z]"," "); //영문자를 제외한 특수문자를 공백으로 처리한다.
            
            String[] token = tmpStr.toUpperCase().split("\\s+"); //텍스트 안의 문자들을 대문자로 처리하고 공백을 기준으로 잘라서 스트링에 넣는다.
                                
            int textidx = 0;
            int textchk = 0;
            int locateidx = 0;
                                
            for(int i = 0; i < token.length; i++)   //텍스트안의 내용을 가져오기 위해 텍스트가 시작하는 index를 구한다.
            {
                if(token[i].equals("TEXT"))
                {
                    if(textchk == 1) 
                        continue;
                    
                    textidx = i;
                    textchk = 1;
                }
                
                if(token[i].equals("LOCATE"))
                {
                    locateidx = i;   
                }
            }
                                
            for(int i=textidx+1;i<locateidx;i++)    //텍스트 안의 대선후보자가 언급되있으면 각각 다른표현으로 되있는 부분을 하나로 통일한다.
            {
                if(token[i].equals(namechk[0]))
                {
                    if(token[i+1].equals(candidate[0]))     
                        token[i] = token[i].replace(namechk[0], " ");
                    else                                    
                        token[i] = token[i].replace(namechk[0], candidate[0]);
                }
                else if(token[i].equals(namechk[1]))
                {
                    if(token[i+1].equals(candidate[1]))
                        token[i] = token[i].replace(namechk[1], " ");
                    else
                        token[i] = token[i].replace(namechk[1], candidate[0]);
                }
                else if(token[i].equals(namechk[2]))
                {
                    if(token[i+1].equals(candidate[2]))
                        token[i] = token[i].replace(namechk[2], " ");
                    else
                        token[i] = token[i].replace(namechk[2], candidate[0]);
                }
                else if(token[i].equals(namechk[3]))
                {
                    if(token[i+1].equals(candidate[3]))
                        token[i] = token[i].replace(namechk[3], " ");
                    else
                        token[i] = token[i].replace(namechk[3], candidate[0]);
                }
                else if(token[i].equals(namechk[4]))
                {
                    if(token[i+1].equals(candidate[4]))
                        token[i] = token[i].replace(namechk[4], " ");
                    else
                        token[i] = token[i].replace(namechk[4], candidate[0]);
                }
                                                            
                for(int j = 0; j < twitid.length; j++)          // 대선후보자들을 twitid로 표현했을경우 사용하고자 하는 대선후보이름으로 바꾼다.
                {
                    if(token[i].equals(twitid[j]))
                        token[i] = token[i].replace(twitid[j], candidate[j]);                                                        
                }
            }

            for(int i = textidx + 1; i < locateidx; i++)                
            {
                for(int j = 0; j < candidate.length; j++)   // 텍스트 안에서 대선 후보 이름이 있는지 검사
                {
                    if(token[i].equals(candidate[j]))
                    {                                    
                        key_word.set(token[i]);             // 대선 후보 이름이 있을 경우, 해당 후보의 이름을 key값으로 한다.

                        for(int k = textidx + 1; k < locateidx; k++)    // 텍스트 내용 검사
                        {
                            // 텍스트에서의 내용 중 공백이나, vote, rt의 단어는 대선후보들과의 연관성이 없다고 생각하여 value에서 제외
                            if(token[k].equals(token[i]) || token[k].equals(" ") || token[k].equals("VOTE") || token[k].equals("RT"))
                                continue;

                            value_word.set(token[k]);               // 같은 대선 후보 단어를 제외한 모든 단어를 value값으로 한다.
                            context.write(key_word, value_word);    // 대선 후보별로 텍스트안의 모든 value 값을 리듀스로 보낸다.
                        } 
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>  //입력키 타입, 입력값 타입, 출력키 타입, 출력값 타입
    { 
        //@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                
            HashMap<String,Integer> hm = new HashMap<String, Integer>();
            
            String ans = "";
            ans = " relation_word        word_number\n\t\t\t\t\t";
                
            // map의 결과물로 Name(CLINTON)={PEOPLE, IMWITHHER, NYPRIMARY,NYPRIMARY,TWEET,...  }의 형식으로 받아온다.
            int total_num = 0;
             
            for(Text val : values)  // 각 후보에 관련된 단어가 몇개씩 존재하는 지 확인하기 위해 HashMap을 선언하여 파악한다.
            {
                String equ_suj = val.toString();
                Integer equ_num = hm.get(equ_suj);
                
                if(equ_num != null) // 단어가 HashMap에 들어있으면 equ_num값에 null값이 아닌 현재 카운트된 수가 들어 있으므로 +1하여 다시 넣어준다.
                    hm.put(equ_suj, ++equ_num);                                                      
                else                // 단어가 HashMap에 들어있지않으면 equ_num값에 null값이 들어가있으므로 1을 넣어준다.                                           
                    hm.put(equ_suj, 1);
                                                                    
                total_num++;
            }
                
            Iterator it = sortByValue(hm).iterator();   // 단어의 종류가 많은 순으로 출력하기 위해 sort를 한다.
            int itNum = 0;
                
            while(it.hasNext() && itNum < 100)
            {
                itNum++;
                            
                String tmpStr = (String)it.next();
                Integer tmpNum = hm.get(tmpStr);
                            
                ans = ans + tmpStr + "\t\t\t" + tmpNum.toString() + "\n\t\t\t\t\t";
            }
                
            context.write(key, new Text(ans));  // 출력형식은 위치 정보(ex. CLINTON)을 처음 한번만 출력하기 위해 key에 위치 정보가 들어가고,
                                                // value는 string형식으로 묶어서 Text형식으로 변환한다.
        }
    }
    
    // 많이 나온 단어 순으로 sort해주는 부분이다.
    public static List sortByValue(final HashMap map)   
    { 
        List<String> list = new ArrayList();
        list.addAll(map.keySet());
      
        Collections.sort(list, new Comparator()
        {
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