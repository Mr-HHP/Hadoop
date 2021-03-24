package defineFormat;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearStation
        implements WritableComparable<YearStation> {
    private String stationid;
    private int year;

    public int compareTo(YearStation o) {
        int n=this.year-o.getYear();
        if(n!=0)return n;
        return this.stationid.compareTo(o.getStationid());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(stationid);
        out.writeInt(year);
    }

    public void readFields(DataInput in) throws IOException {
        this.stationid=in.readUTF();
        this.year=in.readInt();
    }

    @Override
    public String toString() {
        return year+","+stationid;
    }

    public String getStationid() {
        return stationid;
    }

    public void setStationid(String stationid) {
        this.stationid = stationid;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}
