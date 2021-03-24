package defineFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class YearStationRecordReader extends RecordReader {
    private LineRecordReader reader=new LineRecordReader();
    private YearStationPaser pa=new YearStationPaser();
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit,taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!reader.nextKeyValue())return  false;
        Text text=reader.getCurrentValue();
        pa.parser(text.toString());
        if(pa.isValiadTemperture()){
            return true;
        }else {
            return false;
        }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        YearStation ys=new YearStation();
        ys.setStationid(pa.getStationid());
        ys.setYear(pa.getYear());
        return ys;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return new IntWritable(pa.getTemp());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
