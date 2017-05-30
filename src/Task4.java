
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.HashPartitioner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class Task4 implements Serializable {
	static public  JavaSparkContext sc;
	static private class JoinWrapper  implements Serializable{
		private SparkConf conf;
		public JoinWrapper(String inputCustomerFile,String inputOrderFile,String outputFile) throws Exception{
			String master="local[4]";	
			this.conf = new SparkConf()
			        .setAppName(Task4.class.getName())
			        .setMaster(master);
	    	sc = new JavaSparkContext(conf);
		    sc.setLogLevel("ERROR");			    		    
		    final int NUMPARTITIONS=37;		    		    
			HashPartitioner partitioner=new HashPartitioner(NUMPARTITIONS);								
		    JavaPairRDD<Integer,Boolean> customerKeys=sc.textFile(inputCustomerFile)
											.mapToPair(line->{
												String[] splitLine=line.split("\\|");
												return new Tuple2<Integer,Boolean>(Integer.parseInt(splitLine[0]),true);})
											.repartitionAndSortWithinPartitions(partitioner);
			JavaPairRDD<Integer,String> orderTuples=sc.textFile(inputOrderFile)
											.mapToPair(line->{
												String splitLine[]=line.split("\\|");
												return new Tuple2<Integer,String>(Integer.parseInt(splitLine[1]),splitLine[8]);
											}).repartitionAndSortWithinPartitions(partitioner);
		
			
			JavaRDD<Tuple2<Integer,String>>joinedRDD=orderTuples.zipPartitions(customerKeys,(orderIt,customerIt)->{
				List<Tuple2<Integer,String>> joined=new ArrayList<Tuple2<Integer,String>>();

				Tuple2<Integer,String> oid=orderIt.next(); 
				int cid;
				while(customerIt.hasNext()){
					cid=customerIt.next()._1;
					while(oid._1<=cid){	
						if(cid==oid._1)
							joined.add(new Tuple2<Integer, String>(cid,oid._2));
						if(!orderIt.hasNext())break;
						oid=orderIt.next();
					}
					if(orderIt.hasNext()==false) break;
				}
				return joined.iterator();
				});
			
			
			final String temporaryOutputDir="temp";
			File tempDict=new File(temporaryOutputDir);
			if(tempDict.exists())
				FileUtils.deleteDirectory(tempDict);
			FileWriter writer=new FileWriter (new File(outputFile));		  
			joinedRDD.map(x->x._1+","+x._2).saveAsTextFile(temporaryOutputDir);	
			 
			for(int i=0;i<joinedRDD.getNumPartitions();i++){
				String fileName=temporaryOutputDir+"/part-"+String.format("%05d", i);
				BufferedReader br=new BufferedReader(new FileReader(fileName));
				String line;
				while ((line = br.readLine()) != null) {
					writer.append(line+"\n");
				}
				br.close();
			}
			writer.close();
			System.out.println("finished");
			FileUtils.deleteDirectory(tempDict);
			

		}
		
	}
	public static void main(String[] args) throws Exception{
		String inputCustomerFile=args[0];
		String inputOrderFile=args[1];
		String outputFile=args[2];
		JoinWrapper wrapper=new JoinWrapper(inputCustomerFile, inputOrderFile,outputFile);	
	}

}
