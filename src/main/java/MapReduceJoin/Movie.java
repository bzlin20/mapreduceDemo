package MapReduceJoin;

public class Movie {
    private String movieid;
    private String movieName;
    private String moiveType;

    public String getMovieid() {
        return movieid;
    }

    public String getMovieName() {
        return movieName;
    }

    public String getMoiveType() {
        return moiveType;
    }

    public void setMovieid(String movieid) {
        this.movieid = movieid;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public void setMoiveType(String moiveType) {
        this.moiveType = moiveType;
    }

    public Movie(String movieid, String movieName, String moiveType) {
        this.movieid = movieid;
        this.movieName = movieName;
        this.moiveType = moiveType;
    }
}
