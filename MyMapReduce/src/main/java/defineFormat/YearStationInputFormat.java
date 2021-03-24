package defineFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/*
读取器
 */
public class YearStationInputFormat
        extends FileInputFormat<YearStation, IntWritable> {
    @Override
    public RecordReader<YearStation, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        YearStationRecordReader ysr=new YearStationRecordReader();
        ysr.initialize(inputSplit,taskAttemptContext);
        return ysr;
    }
}
