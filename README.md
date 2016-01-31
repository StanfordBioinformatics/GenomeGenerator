# GenomeGenerator
A tool that generates genomics datasets for BigQuery

To generate a new dataset execute the following command:

```
mvn compile exec:java -Dexec.mainClass=com.stanford.dataflow.GenomeGenerator \
-Dexec.args="--runner=BlockingDataflowPipelineRunner \
--project=gbsc-gcp-project-mvp \
--stagingLocation=gs://gbsc-gcp-project-mvp-va_aaa_jobs/dataflow/staging/\
--numWorkers=150 \
--inputTable=gbsc-gcp-project-mvp:simulated_data.aaa_genotype_distributions \
--outputTable=gbsc-gcp-project-mvp:simulated_data.wgs_simulation_100 \
--newGenomeCount=100"
```

If your new dataset is larger than 460 genomes you may want to append extra variants to resemble the expected table size for that number of genomes.  You can append randomly generated variants of chrR to the table like this:

```
 mvn compile exec:java -Dexec.mainClass=com.stanford.dataflow.RandomVariantAppender \
-Dexec.args="--runner=BlockingDataflowPipelineRunner \
--project=gbsc-gcp-project-mvp \
--stagingLocation=gs://gbsc-gcp-project-mvp-va_aaa_jobs/dataflow/staging/ \
--inputTable=gbsc-gcp-project-mvp:simulated_data.wgs_simulation_1000"
```
