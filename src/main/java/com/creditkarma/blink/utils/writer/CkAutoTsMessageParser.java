package com.creditkarma.blink.utils.writer;

/**
 * Created by shengwei.wang on 12/6/16.
 */

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CkAutoTsMessageParser {

    static final String TimestampSampleMax = "2015-01-07T14:22:35.863513-08:00";
    static final String TimestampSampleMin = "2015-01-07T14:22:35-0800";
    static final int TimestampSampleMaxLength = TimestampSampleMax.length();
    static final int TimestampSampleMinLength = TimestampSampleMin.length();
    static final int YEAR_START_INDEX = 0;
    static final int YEAR_END_INDEX = 4;
    static final int MONTH_START_INDEX = 5;
    static final int MONTH_END_INDEX = 7;
    static final int DAY_START_INDEX = 8;
    static final int DAY_END_INDEX = 10;
    static final int HOUR_START_INDEX = 11;
    static final int HOUR_END_INDEX = 13;
    static final int MINUTE_START_INDEX = 14;
    static final int MINUTE_END_INDEX = 16;
    static final int SECOND_START_INDEX = 17;
    static final int SECOND_END_INDEX = 19;

    private static final Pattern UnparseableTsPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})(.?)(\\d{0,6}?)([+-]\\d{2})(:?)(\\d{2})");
    private static final String ParseableTsFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private final boolean TimestampFormatWithMicrosecond;
    private final String enforcedParsingFields;

    private final String timestampName;
    private Set<String> geneticEnforcedParsedFields; // without topic name
    private Set<String> specialEnforcedParsedFields; // topic.field

    public static class TsParseResult {
        public final Date date;
        public final Boolean containsColon;
        public final String year;
        public final String month;
        public final String day;
        public final String hour;
        public final String minute;
        public final String second;
        private long time;

        public void setTime(long time){
            this.time = time;
        }

        public long getTime(){
            return time;
        }

        public TsParseResult(Date date, Boolean containsColon,String year, String month,String day,String hour,String minute,String second) {
            this.date = date;
            this.containsColon = containsColon;
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }
    }

    //enforcedFields may have 2 field formats: 1. just a column name; 2. topic name . field name
    public CkAutoTsMessageParser(String tsName, boolean ifWithMicro, String enforcedFields) {

        timestampName = tsName;
        TimestampFormatWithMicrosecond = ifWithMicro;
        enforcedParsingFields = enforcedFields;
        geneticEnforcedParsedFields = new HashSet<String>();
        specialEnforcedParsedFields = new HashSet<String>();
        if(enforcedParsingFields != null) {
            for(String field : enforcedParsingFields.split(",")) {
                field = field.trim();
                if(field.length()>0) {
                    if(field.indexOf(".") >= 0) {
                        specialEnforcedParsedFields.add(field);
                    }
                    else {
                        geneticEnforcedParsedFields.add(field);
                    }
                }
            }
        }
    }

    // this method DOESN'T mutate the messagePayload
    public TsParseResult extractTimestampMillis(final String messagePayload, final String kafkaTopic) throws Exception {

        TsParseResult result = null;

        JSONObject jsonObject = (JSONObject) JSONValue.parse(messagePayload);
        if (jsonObject != null) {
            for(String col: jsonObject.keySet()) {
                String tableNameAndColumnKey = kafkaTopic + "." + col;
                Object fieldValue = jsonObject.get(col);

                if (fieldValue != null && fieldValue instanceof String) {
                    String fieldStr = (String) fieldValue;
                    if(fieldStr != null && fieldStr.trim().length() > 0) {
                        TsParseResult tsParseRs = null;
                        boolean enforcedParse = geneticEnforcedParsedFields.contains(col)
                                || specialEnforcedParsedFields.contains(tableNameAndColumnKey);

                        if (col.equals(timestampName) || enforcedParse)
                            tsParseRs = parseAsTsString(fieldStr);

                        // get the value from special column "ts", which is the return value of this function
                        if (col.equals(timestampName) && tsParseRs != null && tsParseRs.date != null){
                            tsParseRs.setTime(tsParseRs.date.getTime());
                            result = tsParseRs;
                        }
                    }
                }
            }

        }
        return result;
    }

    /**
     * Parse field value as timestamp string
     *
     * @param tsString
     * @return
     */
    public TsParseResult parseAsTsString(String tsString) throws Exception {
        Date ts = null;
        String year = null;
        String month = null;
        String day = null;
        String hour = null;
        String minute = null;
        String second = null;
        Boolean containsColon = null;
        if (tsString.length() >= TimestampSampleMinLength && tsString.length() <= TimestampSampleMaxLength) {
            // only parse when it is possible in length
            Matcher m = UnparseableTsPattern.matcher(tsString);
            if (m.find()) {
                String parseableTsStr;
                String g3subseconds = m.group(3);
                containsColon = ":".equals(m.group(5));
                if (g3subseconds != null && g3subseconds.trim().length() > 0) {
                    int microseconds = Integer.parseInt(g3subseconds);
                    if (TimestampFormatWithMicrosecond || microseconds >= 1000)
                        g3subseconds =  String.format("%03d", microseconds/1000);
                    else
                        g3subseconds =  String.format("%03d", microseconds) ;
                    parseableTsStr = m.group(1) + m.group(2) + g3subseconds + m.group(4) + m.group(6);
                }
                else {
                    parseableTsStr = m.group(1) + ".0" + m.group(4) + m.group(6);
                }

                year = parseableTsStr.substring(YEAR_START_INDEX,YEAR_END_INDEX);
                month = parseableTsStr.substring(MONTH_START_INDEX,MONTH_END_INDEX);
                day = parseableTsStr.substring(DAY_START_INDEX,DAY_END_INDEX);
                hour = parseableTsStr.substring(HOUR_START_INDEX,HOUR_END_INDEX);
                minute = parseableTsStr.substring(MINUTE_START_INDEX,MINUTE_END_INDEX);
                second = parseableTsStr.substring(SECOND_START_INDEX,SECOND_END_INDEX);
                    ts = new SimpleDateFormat(ParseableTsFormat).parse(parseableTsStr);
            } else{
                throw new Exception("No time stamp pattern found!");
            } // if no meaningful ts found, throw an exception
        } else { // if length not good, throw an exception
            throw  new Exception("time stamp length not good: " + tsString.length());
        }
        return new TsParseResult(ts, containsColon,year,month,day,hour,minute,second);
    }

}