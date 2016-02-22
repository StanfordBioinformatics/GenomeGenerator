/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.stanford.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*  Genome Generator
 * 
 * 	This dataflow pipeline reads genotype distribution information from a BigQuery table
 * 	and outputs randomly generated genomic data to a new BigQuery table based on the 
 * 	probabilities reported in the original table.  Only positions where at least one 
 * 	alternate allele will be output to the new table.  
 */

/* GenomicPosition
 * 
 * This can be loaded with genomic information from the input table and called upon
 * to generate random genotypes with the same probabilities from the input table.
 */
class GenomicPosition {
	public String referenceName = "";
	public String start = "";
	public String end = "";
	public String ref = "";
	public List<String> alt = new ArrayList<String>();
	public double alleleFreq;
	
	// Constructor
	GenomicPosition(String referenceName, String start, String end, String ref, 
			String alt, double alleleFreq) {
		this.referenceName = "chr" + referenceName;
		this.start = String.valueOf(Integer.parseInt(start) - 1);
		this.end = end;
		this.ref = ref;
		this.alt.add(alt);
		//if (alleleFreq == null) {
		//	alleleFreq = 0.0;
		//}
		this.alleleFreq = alleleFreq;
	}
	
	// Print the position to stdout - debugging method
	public void printPosition() {
		String printString = referenceName + " " + start + " " + ref + " " + alt + " " + alleleFreq;
		System.out.println(printString);
	}
	
	public boolean isValid() {
		if (ref == null) {
			return false;
		} else if (alt == null) {
			return false;
		} else if (referenceName == null) {
			return false;
		} else if (start == null) {
			return false;
		} else if (end == null) {
			return false;
		}
		return true;
	}
	
	public boolean isIndel() {
		if (ref.length() > 1) {
			return true;
		} else if (alt.get(0).length() > 1) {
			return true;
		}
		return false;
	}
	
	public boolean isTransition() {
		String alternate = alt.get(0);
		if (ref.equals("A") && alternate.equals("G")) {
			return true;
		} else if (ref.equals("G") && alternate.equals("A")) {
			return true;
		} else if (ref.equals("C") && alternate.equals("T")) {
			return true;
		} else if (ref.equals("T") && alternate.equals("C")) {
			return true;
		}
		return false;
	}
	
	// Decide if this site should be a singleton
	public boolean isSingleton(int genomeCount) {
		if (alleleFreq >= 0.0005) {
			return false;
		}
		
		double veryRareVariants = 550000000.0; 
		double expectedSingletons = 20000.0; 
		double titv = 0.5;
		if (isTransition()) {
			titv = 2.0;

		}
		double pS = (genomeCount * expectedSingletons * titv) / veryRareVariants;	
		double random = Math.random();
		if (random <= pS) {
			return true;
		}
		
		return false;
	}
	

	// Randomly emit a genotype based on the probabilities observed for this position in the input table.
	public List<String> emitGenotype() {
		List<String> randomGenotype = new ArrayList<String>();
		for (int i=0; i<2; i++) {
			double random = Math.random();
			if (random <= alleleFreq) {
				randomGenotype.add("1");
			} else {
				randomGenotype.add("0");
			}
		}
		return(randomGenotype);
	}

	public List<String> emitSingleton() {
		List<String> singleton = new ArrayList<String>();
		for (int i=0; i<2; i++) {
			singleton.add("1");
			singleton.add("0");
		}
		return(singleton);
	}
		
	public List<String> emitRef() {
		List<String> ref = new ArrayList<String>();
		for (int i=0; i<2; i++) {
			ref.add("0");
			ref.add("0");
		}
		return(ref);
	}
	
	// Randomly generate an integer for the AD column
	public List<Integer> randomAD() {
		List<Integer> ads = new ArrayList<Integer>();
		ads.add(getRandomInteger(0, 100));
		return(ads);
	}

	// Randomly generate a list of floats for the likelihood column
	public List<Float> randomLikelihood() {
		List<Float> likelihood = new ArrayList<Float>();
		likelihood.add((float) getRandomInteger(-200, 0));
		return(likelihood);
	}
	
	// Randomly generate an integer for the DP column
	public int randomDP() {
		return(getRandomInteger(10,250));
	}
	
	// Randomly generate a float for the quality column
	public float randomQual() {
		return((float) getRandomInteger(30,250));
	}
	
	// Randomly generate an integer for the GQ column
	public int randomGQ() {
		return(getRandomInteger(10,100));
	}
	
	// Randomly generate an integer for the AD column
	public List<Integer> fastAD() {
		List<Integer> ads = new ArrayList<Integer>();
		ads.add(30);
		return(ads);
	}

	// Randomly generate a list of floats for the likelihood column
	public List<Float> fastLikelihood() {
		List<Float> likelihood = new ArrayList<Float>();
		likelihood.add((float) -30.0);
		return(likelihood);
	}
	
	// Randomly generate an integer for the DP column
	public int fastDP() {
		return(30);
	}
	
	// Randomly generate a float for the quality column
	public float fastQual() {
		return((float) 30);
	}
	
	// Randomly generate an integer for the GQ column
	public int fastGQ() {
		return(30);
	}
	
	// Output 'PASS' for the filter column
	public String getFilter() {
		return("PASS");
	}
	
	// Randomly generate an integer between start and end
	public int getRandomInteger(int start, int end){
		if (start > end) {
			throw new IllegalArgumentException("Start cannot exceed End.");
		}
		Random random = new Random();
		long range = (long)end - (long)start + 1;
		long fraction = (long)(range * random.nextDouble());
		int randomNumber =  (int)(fraction + start);    
		return(randomNumber);
	}
}

/*
 * GenomeGenerator class
 * 
 * This class performs all data management in the dataflow pipeline.
 */
@SuppressWarnings("serial")
public class GenomeGenerator {
	  //private final Aggregator<Long, Long> variantsGenerated =
	  //      createAggregator("variantsGenerated", new Sum.SumLongFn());
		      
	  // Default options - may be set from command line 
	  private static final String GENOTYPE_DISTRIBUTION_TABLE = 
			  "gbsc-gcp-project-mvp:simulated_data.tute_brca1";
			  //"silver-wall-555:TuteTable.hg19";

	  private static final String OUTPUT_TABLE = 
		  "gbsc-gcp-project-mvp:simulated_data.default_output_tute_5";
	  
	  private static final int RANDOM_GENOME_COUNT = 5;

	  /* 
	   * ProcessRowFn
	   * 
	   * Process an individual BigQuery row from the input table.  Load a GenomicPosition object and
	   * output info for a new row.
	   */
	 
	  static List NullArray() {
		  List nullList = new ArrayList();
		  return nullList;
	  }
	  
	  static class ProcessRowFn extends DoFn<TableRow, TableRow> {
		private int randomCount = 0;
		/**
		 * @param randomCount
	     */

 	    public ProcessRowFn(int randomCount) {
 	    	super();
		    this.randomCount = randomCount;
 	    }
	    @Override
	    public void processElement(ProcessContext c) {
	    	
	      // Access information from the input row
	      TableRow row = c.element();
	      String referenceName = (String) row.get("Chr");
	      String start = (String) row.get("Start");
	      String end = (String) row.get("End");
	      String ref = (String) row.get("Ref");
	      String alt = (String) row.get("Alt");
	      Double alleleFreq;
	      if (row.get("X1000G_ALL") == null) {
	    	  alleleFreq = 0.0;
	      } else {
	    	  alleleFreq = Double.parseDouble(row.get("X1000G_ALL").toString());
	    	  //meanTemp =   Double.parseDouble(row.get("mean_temp" ).toString());
	      }
	      
	      // Create a new position object with row information
	      GenomicPosition position = new GenomicPosition(referenceName, start, end, ref, alt, alleleFreq);
	      
	      // Generate genomic data for the requested number of genomes.  Store call-subrows for insertion into main row later
	      //position.printPosition();
	      if (!position.isValid()) {
	    	  return;
	      }
	      
	      
	      TableRow newRow = null;
	      if (position.alleleFreq < 0.0005) {
	    	  if (position.isSingleton(randomCount)) {
	    		  newRow = generateSingletonRow(position, randomCount);
	    	  } else {
	    		  //System.out.println("too rare");
	    		  return;
	    	  }
	      } else {
	    	  //System.out.println("generating row");
	    	  newRow = generateRow(position, randomCount);
	      }
	      if (newRow.size() == 0) {
	    	  return;
	      }
	      c.output(newRow);
	  
	    }

	  } 
	  
	  private static TableRow generateRow(GenomicPosition position, int sampleCount) {
    	  List<TableRow> calls = new ArrayList<>();
    	  
    	  int altCount = 0;
    	  for (int i=1; i <= sampleCount; i++) {
    		  String call_set_name = "R" + String.format("%06d", i);
    		  List<String> randomGenotype = position.emitGenotype();
    		  if (randomGenotype != null && randomGenotype.contains("1")) {
    			  altCount += 1;
    		  }
    		  TableRow call = new TableRow()
    				  .set("call_set_name", call_set_name)
    				  .set("genotype", randomGenotype)
    				  .set("AD", position.fastAD())
    				  .set("DP", position.fastDP())
    				  .set("QUAL", position.fastQual())
    				  .set("GQ", position.fastGQ())
    				  .set("genotype_likelihood", position.fastLikelihood())
    				  .set("FILTER", position.getFilter())
    				  .set("QC", NullArray());
	      
    		  calls.add(call);
    	  }
    		  
    		  // If no alternate alleles are generated, don't insert a row
    	  if (altCount == 0) {
    		  TableRow emptyRow = new TableRow();
    		  return emptyRow;
    	  }
    	  
    	  // Define complete row
    	  TableRow newRow = new TableRow()
    			  .set("reference_name", position.referenceName)
    			  .set("start", position.start)
    			  .set("end", position.end)
    			  .set("reference_bases", position.ref)
    			  .set("alternate_bases", position.alt)
    			  .set("quality", position.randomQual())
    			  .set("QC", NullArray())
    			  .set("names", NullArray())
    			  .set("filter", NullArray())
    			  .set("call", calls);
    	  
    	  return newRow;
	  }
	  
	  private static TableRow generateSingletonRow(GenomicPosition position, int sampleCount) {
    	  List<TableRow> calls = new ArrayList<>();
    	  
    	  String singletonSample = "R" + String.format("%06d", position.getRandomInteger(0, sampleCount));
    	  
    	  for (int i=1; i <= sampleCount; i++) {
    		  String call_set_name = "R" + String.format("%06d", i);
    		  List<String> genotype = new ArrayList<String>();
    		  if (call_set_name == singletonSample) {
    			  genotype = position.emitSingleton();
    		  } else {
    			  genotype = position.emitRef();
    		  }
    		  TableRow call = new TableRow()
    				  .set("call_set_name", call_set_name)
    				  .set("genotype", genotype)
    				  .set("AD", position.randomAD())
    				  .set("DP", position.randomDP())
    				  .set("QUAL", position.randomQual())
    				  .set("GQ", position.randomGQ())
    				  .set("genotype_likelihood", position.randomLikelihood())
    				  .set("FILTER", position.getFilter())
    				  .set("QC", NullArray());
    		  calls.add(call);
    	  }
    		  
    	  
    	  // Define complete row
    	  TableRow newRow = new TableRow()
    			  .set("reference_name", position.referenceName)
    			  .set("start", position.start)
    			  .set("end", position.end)
    			  .set("reference_bases", position.ref)
    			  .set("alternate_bases", position.alt)
    			  .set("quality", position.randomQual())
    			  .set("QC", NullArray())
    			  .set("names", NullArray())
    			  .set("filter", NullArray())
    			  .set("call", calls);
    	  
    	  return newRow;
	  }
	  
	  static class GenerateGenomes
	      extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		private int randomCount = 0;
		/**
		 * @param randomCount
		 */

	 	public GenerateGenomes(int randomCount) {
	 	    super();
			this.randomCount = randomCount;
	 	}
	    @Override
	    public PCollection<TableRow> apply(PCollection<TableRow> rows) {

	      PCollection<TableRow> results = rows.apply(
	          ParDo.of(new ProcessRowFn(randomCount)));

	      return results;
	    }
	  }
	  
	  // Options
	  private static interface Options extends PipelineOptions {
	    @Description("Table to read from, specified as "
	        + "<project_id>:<dataset_id>.<table_id>")
	    @Default.String(GENOTYPE_DISTRIBUTION_TABLE)
	    String getInputTable();
	    void setInputTable(String inputTable);

	    @Description("BigQuery table to write to, specified as "
	        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
	    @Default.String(OUTPUT_TABLE)
	    String getOutputTable();
	    void setOutputTable(String outputTable);
	    
	    @Description("Number of genomes to generate")
		@Default.Integer(RANDOM_GENOME_COUNT)
	    Integer getNewGenomeCount();
		void setNewGenomeCount(Integer newGenomeCount);
		
		@Description("Subsetting query")
		//@Default.String("SELECT * FROM [gbsc-gcp-project-mvp:simulated_data.tute_brca1] LIMIT 1000")
	    String getSubsetQuery();
		void setSubsetQuery(String subsetQuery);
	
		@Description("Whether to append to an existing BigQuery table.")
		@Default.Boolean(false)
		boolean getAppendToTable();	
		void setAppendToTable(boolean value);
		
	  }
	  
	  private static TableSchema getTableSchema() {
		    List<TableFieldSchema> callFields = new ArrayList<>();
		    callFields.add(new TableFieldSchema().setName("call_set_name").setType("STRING"));
		    callFields.add(new TableFieldSchema().setName("phaseset").setType("STRING"));
		    callFields.add(new TableFieldSchema().setName("genotype").setType("INTEGER")
		        .setMode("REPEATED"));
		    callFields.add(new TableFieldSchema().setName("genotype_likelihood").setType("FLOAT")
		        .setMode("REPEATED"));
		    callFields.add(new TableFieldSchema().setName("AD").setType("INTEGER").setMode("REPEATED"));
		    callFields.add(new TableFieldSchema().setName("DP").setType("INTEGER"));
		    callFields.add(new TableFieldSchema().setName("FILTER").setType("STRING"));
		    callFields.add(new TableFieldSchema().setName("GQ").setType("INTEGER"));
		    callFields.add(new TableFieldSchema().setName("QUAL").setType("FLOAT"));
		    callFields.add(new TableFieldSchema().setName("QC").setType("STRING").setMode("REPEATED"));

		    List<TableFieldSchema> fields = new ArrayList<>();
		    fields.add(new TableFieldSchema().setName("variant_id").setType("STRING"));
		    fields.add(new TableFieldSchema().setName("reference_name").setType("STRING"));
		    fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		    fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		    fields.add(new TableFieldSchema().setName("reference_bases").setType("STRING"));
		    fields.add(new TableFieldSchema().setName("alternate_bases").setType("STRING")
		        .setMode("REPEATED"));
		    fields.add(new TableFieldSchema().setName("names").setType("STRING").setMode("REPEATED"));
		    fields.add(new TableFieldSchema().setName("filter").setType("STRING").setMode("REPEATED"));
		    fields.add(new TableFieldSchema().setName("quality").setType("FLOAT"));
		    fields.add(new TableFieldSchema().setName("QC").setType("STRING").setMode("REPEATED"));
		    fields.add(new TableFieldSchema().setName("call").setType("RECORD").setMode("REPEATED")
		        .setFields(callFields));

		    return new TableSchema().setFields(fields);
		  }

	  public static void main(String[] args) {
	    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

	    Pipeline p = Pipeline.create(options);

	    String subsetQuery = options.getSubsetQuery();
	    if (subsetQuery == null) {
	    
	    	p.apply(BigQueryIO.Read.from(options.getInputTable()))
	    	.apply(new GenerateGenomes(options.getNewGenomeCount()))
	    	.apply(BigQueryIO.Write
	    			.to(options.getOutputTable())
	    			.withSchema(getTableSchema())
	    			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
	    } else {
	    	p.apply(BigQueryIO.Read.fromQuery(subsetQuery))
	    	.apply(new GenerateGenomes(options.getNewGenomeCount()))
	    	.apply(BigQueryIO.Write
	    			.to(options.getOutputTable())
	    			.withSchema(getTableSchema())
	    			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    			.withWriteDisposition(options.getAppendToTable()
	    					 ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
	    }

	    p.run();
	  }
	}
