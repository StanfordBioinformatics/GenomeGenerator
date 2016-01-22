package com.stanford.genomegenerator;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
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
	private int total = 0;
	private String genotypeCounts = "";
	private List<String> genotypes = new ArrayList<String>();
	private List<Integer> counts = new ArrayList<Integer>();
	private List<Float> probabilities = new ArrayList<Float>();
	
	// Constructor
	GenomicPosition(String referenceName, String start, String end, String ref, 
			String alt, String genotypeCounts, int total) {
		this.referenceName = referenceName;
		this.start = start;
		this.end = end;
		this.ref = ref;
		this.alt.add(alt);
		this.genotypeCounts = genotypeCounts;
		this.total = total;
		try {
			setDistributions();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Print the position to stdout - debugging method
	public void printPosition() {
		String printString = referenceName + " " + start + " " + ref + " " + alt + " " + genotypeCounts;
		String dists = referenceName + " " + start;
		for (int i=0; i<genotypes.size(); i++) {
			dists = dists + " " + genotypes.get(i) + ":" + counts.get(i) + ":" + probabilities.get(i);
		}
		System.out.println(printString);
		System.out.println(dists);
	}
	
	// Set the genotype distributions based on the counts column from the input table
	// Keep three lists of genotypes, counts, and probabilities.  Each index in each table
	// corresponds with a genotype.
	private void setDistributions() throws IOException {
		  int found = 0;
		  // Collect input
	      for (String genotypeAndCount: genotypeCounts.split(";")){
	    	  String[] parts = genotypeAndCount.split(":");
	    	  genotypes.add(parts[0]);
	    	  int count = Integer.parseInt(parts[1]);
	    	  counts.add(count);
	    	  found += count;
	       }
	      if (found < total) {
	    	  genotypes.add(null);
	    	  counts.add(total - found);
	      } else if (total < found) {
	    	  throw new IOException("Unexpected number of genotypes");
	      }
	      double previous = 0;
	      for(int c : counts) {
	    	  double probability =  ((double) c / (double) total);
	    	  probabilities.add((float) (probability + previous));
	    	  previous = previous + probability;
	      }
	}
	
	// Randomly emit a genotype based on the probabilities observed for this position in the input table.
	public List<String> emitGenotype() {
		double random = Math.random();
		List<String> randomGenotype = new ArrayList<String>();
		for (int i=0; i<genotypes.size(); i++) {
			if (random <= (probabilities.get(i))) {
				if (genotypes.get(i) == null) {
					return(null);
				}
				String[] alleles = genotypes.get(i).split(",");
				for (String a: alleles) {
					randomGenotype.add(a);
				}
				break;
			}
		}
		return(randomGenotype);
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
	
	// Output 'PASS' for the filter column
	public String getFilter() {
		return("PASS");
	}
	
	// Randomly generate an integer between start and end
	private int getRandomInteger(int start, int end){
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
	  // Default options - may be set from command line 
	  private static final String GENOTYPE_DISTRIBUTION_TABLE = 
	      "gbsc-gcp-project-mvp:simulated_data.aaa_genotypte_distributions_brca1";

	  private static final String OUTPUT_TABLE = 
		  "gbsc-gcp-project-mvp:simulated_data.default_output";
	  
	  private static final int GENOME_COUNT = 460; // Need to put this in the table - hard coding for now.
	  
	  private static final int RANDOM_GENOME_COUNT = 10;

	  /* 
	   * ProcessRowFn
	   * 
	   * Process an individual BigQuery row from the input table.  Load a GenomicPosition object and
	   * output info for a new row.
	   */
	  
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
	      String referenceName = (String) row.get("reference_name");
	      String start = (String) row.get("start");
	      String end = (String) row.get("end");
	      String ref = (String) row.get("reference_bases");
	      String alt = (String) row.get("alts");
	      String genotypeCounts = (String) row.get("counts");
	      
	      // Create a new position object with row information
	      GenomicPosition position = new GenomicPosition(referenceName, start, end, ref, alt, genotypeCounts, GENOME_COUNT);
	      
	      // Generate genomic data for the requested number of genomes.  Store call-subrows for insertion into main row later
	      List<TableRow> calls = new ArrayList<>();
	      int altCount = 0;
	      for (int i=1; i <= randomCount; i++) {
	    	  String call_set_name = "R" + String.format("%06d", i);
	    	  List<String> randomGenotype = position.emitGenotype();
	    	  if (randomGenotype != null && randomGenotype.contains("1")) {
	    		  altCount += 1;
	    	  }
		      TableRow call = new TableRow()
		                .set("call_set_name", call_set_name)
		                .set("genotype", randomGenotype);
		      			//.set("AD", position.randomAD())
		      			//.set("DP", position.randomDP())
		      			//.set("QUAL", position.randomQual())
		      			//.set("GQ", position.randomGQ())
		      			//.set("genotype_likelihood", position.randomLikelihood())
		      			//.set("FILTER", position.getFilter());
		      calls.add(call);
	      }
	      
	      // If no alternate alleles are generated, don't insert a row
	      if (altCount == 0) {
	    	  return;
	      }
	      
	      // Define complete row
	      TableRow newRow = new TableRow()
		          .set("reference_name", position.referenceName)
		          .set("start", position.start)
		          .set("end", position.end)
		          .set("reference_bases", position.ref)
		          .set("alternate_bases", position.alt)
		          //.set("quality", position.randomQual())
	      		  .set("call", calls);
	      
	      c.output(newRow);
	    }
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
	    String getInput();
	    void setInput(String value);

	    @Description("BigQuery table to write to, specified as "
	        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
	    @Default.String(OUTPUT_TABLE)
	    String getOutput();
	    void setOutput(String value);
	    
	    @Description("Number of genomes to generate")
		@Default.Integer(RANDOM_GENOME_COUNT)
	    Integer getRandomGenomeCount();
		void setRandomGenomeCount(Integer value);
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

	    p.apply(BigQueryIO.Read.from(options.getInput()))
	     .apply(new GenerateGenomes(options.getRandomGenomeCount()))
	     .apply(BigQueryIO.Write
	        .to(options.getOutput())
	        .withSchema(getTableSchema())
	        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

	    p.run();
	  }
	}
