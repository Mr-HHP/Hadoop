package defineFormat;
/*
解析器，读取一行提取，年份，气象站编号，温度
 */
public class YearStationPaser {
    private static final int MISS=9999;
    private String stationid;
    private int year;
    private int temp;
    private boolean isValiadTemperture;

    //str一行记录
    public void parser(String str){
        if(str.length()<93){
            isValiadTemperture=false;
            return;
        }
        stationid=str.substring(0,15);
        year=Integer.parseInt(str.substring(15,19));
        if(str.charAt(87)=='+'){
            temp=Integer.parseInt(str.substring(88,92));
        }else{
            temp=Integer.parseInt(str.substring(87,92));
        }
        String quality=str.substring(92,93);
        if(temp!=MISS&&quality.matches("[01459]")){
            isValiadTemperture=true;
        }else{
            isValiadTemperture=false;
        }
    }

    public boolean isValiadTemperture() {
        return isValiadTemperture;
    }

    public void setValiadTemperture(boolean valiadTemperture) {
        isValiadTemperture = valiadTemperture;
    }

    public static int getMISS() {
        return MISS;
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

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }
}
