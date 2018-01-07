/**
 * Operator Mapper for mapping paired reads to a
 * reference genome (path supplied by properties.xml)
 *
 *  Pre-Requisites
 * -> Reference Genome need to be indexed using BWA. (BWA -index ${PATH_NAME})
 *
 *  Tasks:
 * -> 1. Loads reference from path suppplied.
 * -> 2. Gets input from LineByLineInputOperator
 * -> 3. Takes sequenceid, sequence and qual from input port.
 * -> 4. Begin Mapping to reference.
 * -> 5. Send Mapped reads to GenericFileOutputOperator for writing aligned reads as SAM.
 *
 * Time duration LOGS available in dt.log
 * Logs and messages from native library can be found in std.err
 *
 * Java Bindings (JNI) for BWA by Dr. Pierre Lindenbaum
 * https://github.com/lindenb/jbwa
 */

package com.singlefastqmapper;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;


public class Mapper extends BaseOperator implements Operator.ActivationListener
{

    private BwaIndex index;
    private BwaMem mem;
    private static int counter=0;
    private String seqid;
    private byte[] seq;
    private byte[] qual;
    private ShortRead read;
    private String refName;
    private String outputFilePath;
    private Boolean isValidRefPath = true;
    @NotNull
    private String refFilePath;

    private Date app_start;
    private Date app_stop;
    private Date ref_start;
    private Date ref_stop;
    private Date map_start;
    private Date map_stop;

    private static final Logger LOG = LoggerFactory.getLogger(Mapper.class);

    public final transient DefaultOutputPort<String> outputPort= new DefaultOutputPort<>();

    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>()
    {
        @Override
        public void process(String line)
        {
            switch(counter % 4)
            {
                case 0: seqid = line;
                        ++counter;
                        break;

                case 1: seq = line.getBytes();
                        ++counter;
                        break;

                case 2: ++counter;
                        break;

                case 3: qual = line.getBytes();
                        read = new ShortRead(seqid, seq, qual);
                        ++counter;

                        try
                        {
                            for (AlnRgn a : mem.align(read))
                            {
                                if (a.getSecondary() >= 0)
                                    continue;

                                String sequence = new String(read.getBases());
                                String qual = new String(read.getQualities());

                                outputPort.emit(read.getName() + "\t" + a.getAs() + "\t" +  a.getChrom() + "\t" + a.getPos() + "\t" + a.getMQual() + "\t" + a.getCigar() + "\t" + "*" + "\t" + 0 + "\t" + 0 + "\t" + sequence + "\t" + qual +  "\tNM:i:" +  a.getNm() );
                            }
                        }

                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }

                        break;
            }
        }
    };


    @Override
    public void activate(Context context)
    {
        //JNI for BWA
        System.load("/home/plaban/Downloads/jbwa-master/src/main/native/libbwajni.so");

        app_start = new Date();

        try
        {
            LOG.info("Loading Reference Genome and its Indices......");

            ref_start = new Date();
            File refFile = new File(getRefFilePath());
            setRefName(refFile.getName());

            if (refFile.isFile())
            {
                index = new BwaIndex(refFile);
                mem = new BwaMem(index);

                ref_stop = new Date();

                LOG.info("Time elapsed in loading genome: " + (ref_stop.getTime() - ref_start.getTime()) + "ms");
                LOG.info("Beginning to map reads to reference genome...... " + getRefName());
            }

            else
            {
                isValidRefPath = false;
                LOG.error("Error!! Invalid Path: " + getRefFilePath());
            }


            /*BufferedReader reader = new BufferedReader(new FileReader(refFile));
            refName = reader.readLine();

            if (refName.charAt(0) == '>')
                refName = refName.substring( 1, refName.length());*/
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }

        map_start = new Date();
    }

    public String getRefFilePath()
    {
        return refFilePath;
    }

    public void setRefFilePath(String path)
    {
        this.refFilePath = path;
    }

    public String getRefName()
    {
        return refName;
    }

    public void setRefName(String refName)
    {
        this.refName = refName;
    }

    @Override
    public void deactivate()
    {
        app_stop = new Date();
        map_stop = new Date();

        if (isValidRefPath)
            LOG.info("Teardown of Mapping operation......... Started");

        index.close();
        mem.dispose();

        if (isValidRefPath)
        {
            LOG.info("Teardown of Mapping operation......... Finished");
            LOG.info("**********************SUMMARY**********************");
            LOG.info("Time elapsed in loading genome: " + (ref_stop.getTime() - ref_start.getTime()) + "ms");
            LOG.info("Time elapsed in mapping reads to genome: " + (map_stop.getTime() - map_start.getTime()) + "ms");
            LOG.info("Total time taken by the Operator: " + (app_stop.getTime() - app_start.getTime()) + "ms");
        }

        else
            LOG.info("Mapping operation unsuccessful........ File not found at " + getRefFilePath());
    }
}
//End_Of_Code