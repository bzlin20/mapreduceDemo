package MapReduceJoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRate implements WritableComparable<MovieRate> {
    private String movieid;
    private String useid;
    private int rate;
    private String movieName;
    private String movieType;
    private long ts;

    public String getMovieid() {
        return movieid;
    }

    public String getUseid() {
        return useid;
    }

    public int getRate() {
        return rate;
    }

    public String getMovieName() {
        return movieName;
    }

    public String getMovieType() {
        return movieType;
    }



    public void setMovieid(String movieid) {
        this.movieid = movieid;
    }

    public void setUseid(String useid) {
        this.useid = useid;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public void setMovieType(String movieType) {
        this.movieType = movieType;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public MovieRate(String movieid, String useid, int rate, String movieName, String movieType,long ts) {
        this.movieid = movieid;
        this.useid = useid;
        this.rate = rate;
        this.movieName = movieName;
        this.movieType = movieType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return   movieid + "\t" + useid + "\t" + rate + "\t" + movieName
                + "\t" + movieType + "\t" + ts;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(movieid);
        out.writeUTF(useid);
        out.writeInt(rate);
        out.writeUTF(movieName);
        out.writeUTF(movieType);
        out.writeLong(ts);
    }

    public void readFields(DataInput in) throws IOException {
        this.movieid = in.readUTF();
        this.useid = in.readUTF();
        this.rate = in.readInt();
        this.movieName = in.readUTF();
        this.movieType = in.readUTF();
        this.ts = in.readLong();
    }
    public int compareTo(MovieRate o) {
        int it = o.getMovieid().compareTo(this.movieid);
        if(it == 0){
            return o.getUseid().compareTo(this.useid) ;
        }else{
            return it;
        }
    }
}
