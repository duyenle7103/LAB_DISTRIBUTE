package datatype;

import java.util.Date;

public interface Environment {
    public String getType();
    public Date getTime();
    public String getStation();
    public String toStr();
    public String toCSV();
}