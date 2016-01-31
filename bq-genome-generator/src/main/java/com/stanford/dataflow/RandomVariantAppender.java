package com.stanford.dataflow;



/**
 * 
 */
/**
 * @author gmcinnes
 *
 */


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.eclipse.jetty.util.log.Log;


class RandomGenomicPosition {
	public String referenceName = "";
	public String start = "";
	public String end = "";
	public String ref = "";
	public List<String> alt = new ArrayList<String>();
	
	// Constructor
	RandomGenomicPosition(String referenceName, String start) {
		this.referenceName = referenceName;
		this.start = start;
		this.end = Integer.toString(Integer.parseInt(start) + 1);
		setReference();
		setAlternate();
	}
	
	public void printVariant() {
		String printString = referenceName + " " + start + " " + end + " " + ref + " " + alt;
		System.out.println(printString);
	}
 	
	private void setReference() {
		this.ref = randomNucleotide();
	}
	
	private void setAlternate() {
		String alt = null;
		if (isTransition()) {
			alt = transition(ref);
		} else {
			alt = transversion(ref);
		}
		this.alt.add(alt);
	}
	
	
	private String randomNucleotide() {
		List<String> nucleotides = nucleotides();
		List<Float> frequencies = nucleotideFrequencies();
		Float random = (float) Math.random();
		for (int i = 0; i < nucleotides.size(); i++) {
			if (random <= frequencies.get(i)) {
				return nucleotides.get(i);
			}
		}
		// else - something went wrong
		System.out.println(random);
		System.out.println(nucleotides);
		System.out.println(frequencies);
		return "A";
	}
	
	private List<String> nucleotides() {
		List<String> alleles = new ArrayList<String>();
		alleles.add("A"); 
		alleles.add("C");
		alleles.add("T");
		alleles.add("G");
		return alleles;
	}
	
	private List<Float> nucleotideFrequencies() {
		// Frequencies based on genome-wide GC content of roughly 0.4
		// order matches order in nucleotides() - ACTG
		List<Float> freqs = new ArrayList<Float>();
		freqs.add((float) 0.3);
		freqs.add((float) 0.5);
		freqs.add((float) 0.8);
		freqs.add((float) 1.0);
		return freqs;
	}
	
	private boolean isTransition() {
		double random = Math.random();
		if (random <= 0.333) {
			return false;
		}
		return true;
	}
	
	private String transition(String ref) {
		if (ref == "A") {
			return "G";
		} else if (ref == "G") {
			return "A";
		} else if (ref == "C") {
			return "T";
		} else if (ref == "T") {
			return "C";
		} 
		// nothing found - something's wrong
		return ref;
	}
	
	private String transversion(String ref) {
		// there are specific probabilities for each transversion, 
		// but we'll just give a 50/50 chance for each
		if (ref == "A") {
			String[] alts = {"C", "T"};
			String alt = randomAllele(alts);
			return alt;
		} else if (ref == "C") {
			String[] alts = {"A", "G"};
			String alt = randomAllele(alts);
			return alt;
		} else if (ref == "T") {
			String[] alts = {"A", "C"};
			String alt = randomAllele(alts);
			return alt;		
		} else if (ref == "G") {
			String[] alts = {"C", "T"};
			String alt = randomAllele(alts);
			return alt;
		}
		// else... something's wrong - left as an exercise for the user
		return ref;
	}
	
	private String randomAllele(String[] alts) {
		int idx = new Random().nextInt(alts.length);
		String random = (alts[idx]);
		return random;
	}
	
	// Randomly emit a genotype.  The probability of an alternate allele is 1 in 2000
	public List<String> emitGenotype() {
		List<String> genotype = new ArrayList<String>();
		for (int i = 0; i < 2; i++) {
			double random = Math.random();
			if (random < 0.0005) {
				genotype.add("1");
			} else {
				genotype.add("0");
			}
		}
		return(genotype);
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
	


public class RandomVariantAppender {
	  //private final Aggregator<Long, Long> variantsGenerated =
		//createAggregator("variantsGenerated", new Sum.SumLongFn());
		//  private final Aggregator<Long, Long> samplesProcessed =
		  //    createAggregator("samplesProcessed", new Sum.SumLongFn());;

	
	  // Default options - may be set from command line 
	  private static final String THE_TABLE = "gbsc-gcp-project-mvp:simulated_data.default_output_500";

	  static class ExtractSampleIDs extends DoFn<TableRow, String> {
		    @Override
		    public void processElement(ProcessContext c){
		      TableRow row = c.element();
		      String callSetName = (String) row.get("call_set_name");
		      String genomeCount = (String) row.get("genome_count");
		      String sampleKey = callSetName + "-" + genomeCount;
		      c.output(sampleKey);
		    }
		  }
	  
	  @SuppressWarnings("serial")
	  static class GenerateVariantKeys extends DoFn<String, String> {
		   @Override
		   public void processElement(ProcessContext c) {
		    	String sampleKey = c.element();
		    	String[] splits = sampleKey.split("-");
		    	int sampleNumber = Integer.parseInt(splits[0].substring(1));
		    	int counts = Integer.parseInt(splits[1]);
		    	int newVariantCount = perSampleVariantCount(counts);
		    	if (newVariantCount == 0) {
		    		return;
		    	}
		    	for (int i = 1; i <= newVariantCount; i++) {
		    		int variantPos = (sampleNumber * newVariantCount) + i;
		    		String variantKey = sampleKey + "-" + Integer.toString(variantPos);
		    		c.output(variantKey);
		    	}
		    	//samplesProcessed.addValue(1L);
		   }
	  }
	  
	  static int perSampleVariantCount(int sampleCount) {
		  if (sampleCount <= 460) {
			  return 0;
		  }
		  // Total number of variants expected
		  double tatalNewVariantCount = 2336137 + (2052329 * Math.pow(sampleCount, (1/2.5)));
		  // Minus the number found in the AAA dataset
		  double scaledNewVariantCount = tatalNewVariantCount - 26178545;
		  // Divided by the sample count 
		  int perSampleCount = (int) (scaledNewVariantCount / sampleCount);		  		  
		  return perSampleCount;
	  }
	  
	  // Take in variant keys, output BigQuery row for variant.  Randomly assign some samples calls
	  // Input format: sample_id-count-variant_id
	  @SuppressWarnings("serial")
	  static class GenerateRow extends DoFn<String, TableRow> {
		    @SuppressWarnings("null")
			@Override
		    public void processElement(ProcessContext c) {
		    	String variantKey = c.element();
		    	String[] splits = variantKey.split("-");
		    	String sampleId = splits[0];
		    	int sampleCount = Integer.parseInt(splits[1]);
		    	String variantNumber = splits[2];
		    	String sampleNumber = sampleId.substring(1);
		    	
		    	RandomGenomicPosition position = new RandomGenomicPosition("chrR", variantNumber);
		    	
			    List<TableRow> calls = new ArrayList<>();
			    for (int i=1; i <= sampleCount; i++) {
			    	String call_set_name = "R" + String.format("%06d", i);
			    	List<String> randomGenotype = null;
			    	if (call_set_name == sampleId) {
			    		randomGenotype.add("0");
			    		randomGenotype.add("1");
			    	} else {
			    		randomGenotype = position.emitGenotype();
			    	}

				    TableRow call = new TableRow()
				    		.set("call_set_name", call_set_name)
				    		.set("genotype", randomGenotype)
				    		.set("AD", position.randomAD())
				      		.set("QUAL", position.randomQual())
				      		.set("DP", position.randomDP())
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
			      
			      //variantsGenerated.addValue(1L);
			      c.output(newRow);
		    	
		    }
	  }
	  
	  static class SNPGenerator
      extends PTransform<PCollection<TableRow>, PCollection<String>> {
		  @Override
		  public PCollection<String> apply(PCollection<TableRow> rows) {

			  PCollection<String> sampleIds = rows.apply(
					  ParDo.of(new ExtractSampleIDs()));

			  PCollection<String> variants = sampleIds.apply(
					  ParDo.of(new GenerateVariantKeys()));
      
			  return variants;
		  }
	  }
  
	  static class SNPRandomizer
    		extends PTransform<PCollection<String>, PCollection<TableRow>> {
		  @Override
		  public PCollection<TableRow> apply(PCollection<String> variant_ids) {

			  PCollection<TableRow> rows = variant_ids.apply(
					  ParDo.of(new GenerateRow()));
			  return rows;
		  }
	  }
	
	  @SuppressWarnings("rawtypes")
	  static List NullArray() {
		  List nullList = new ArrayList();
		  return nullList;
	  }
	  
	  // Options
	  private static interface Options extends PipelineOptions {
	    @Description("Table to read from, specified as "
	        + "<project_id>:<dataset_id>.<table_id>")
	    @Default.String(THE_TABLE)
	    String getInputTable();
	    void setInputTable(String inputTable);
	  }
	  
	  private static String query(String table) {
		  String query = "SELECT\n" +
				  			"call_set_name,\n" +
				  			"genome_count\n" +
				  		"FROM\n" +
				  			"(SELECT call.call_set_name AS call_set_name FROM \n" +
				  				"[" + table + "]" +
				  				" GROUP BY call_set_name) AS sample_ids\n" +
				  			"CROSS JOIN (\n" +
				  				"SELECT\n" +
				  					"COUNT(*) AS genome_count\n" +
				  				"FROM (\n" +
				  					"SELECT\n" +
				  						"call.call_set_name\n" +
				  					"FROM\n" +
				  						"[" + table + "]" +
				  				" GROUP BY call.call_set_name)) AS count";
		  return query;
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

	    String query = query(options.getInputTable());
	    
	    Pipeline p = Pipeline.create(options);

	    p.apply(BigQueryIO.Read.fromQuery(query))
	     .apply(new SNPGenerator())
	     .apply(new SNPRandomizer())
	     .apply(BigQueryIO.Write
	        .to(options.getInputTable()) 
	        .withSchema(getTableSchema())
	        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

	    p.run();
	  }
}


