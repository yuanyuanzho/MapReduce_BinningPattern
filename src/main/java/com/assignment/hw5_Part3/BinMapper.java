package com.assignment.hw5_Part3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class BinMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private MultipleOutputs<Text, NullWritable> mos = null;
	
	protected void setup(Context context){
		mos = new MultipleOutputs(context);
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{

		Pattern p = Pattern.compile("\".+\"");
        Matcher m = p.matcher(value.toString());
		
	    if(m.find()){
	    	String[] contents = m.group(0).split(" ");
            String type = contents[0].substring(1);
        
			if (type.equalsIgnoreCase("GET")) {
			    mos.write("bins", value, NullWritable.get(),"GET");
			}else if (type.equalsIgnoreCase("POST")) {
				mos.write("bins", value, NullWritable.get(),"POST");
			}else if(type.equalsIgnoreCase("ABCD")){
				mos.write("bins", value, NullWritable.get(),"ABCD");
			}else if(type.equalsIgnoreCase("HEAD")){
				mos.write("bins", value, NullWritable.get(),"HEAD");
			}else{
				mos.write("bins", value, NullWritable.get(),"OTHERS");
			}
		}

	}
	protected void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}
