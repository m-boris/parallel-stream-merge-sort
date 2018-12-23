package com.company.data;


import org.apache.commons.lang3.StringUtils;

public class LineData {
    private String id;
    private String value;



    public LineData(String line) {
        line = line.trim();
        id = StringUtils.substringBefore( line, "," );
        value = StringUtils.substringAfter( line, "," );
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static boolean isLineData( String str ){
        return StringUtils.isNotBlank( str ) &&  str.indexOf( "," )>0;
    }

    public String getLineAsString(){
        return id + "," + value;
    }

}
