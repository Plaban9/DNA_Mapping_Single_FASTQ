/**
 * Method to create or set-up flow of data (DAG)
 */
package com.singlefastqmapper;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

@ApplicationAnnotation(name="Single_FASTQ_Mapper")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Operators
    //Reads FASTQ files from directory (input path to be set in properties.xml)
    LineByLineFileInputOperator fastqReader = dag.addOperator("FASTQ_Reader", LineByLineFileInputOperator.class);

    //Loads a Reference Genome file(FASTA) and maps FASTQ reads to the reference Genome
    Mapper mapper =dag.addOperator("Mapper", Mapper.class);

    //Writes the mapped data as SAM file (output directory path and and file name tobe set in properties.xml)
    GenericFileOutputOperator.StringFileOutputOperator fileOutput = dag.addOperator("SAM_Writer", GenericFileOutputOperator.StringFileOutputOperator.class);

    //Stream(s)
    dag.addStream("toMapper", fastqReader.output, mapper.inputPort);
    dag.addStream("toSAMWriter", mapper.outputPort, fileOutput.input);


  }
}
