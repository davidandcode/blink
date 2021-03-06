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

        public TsParseResult(Date date, Boolean containsColon) {
            this.date = date;
            this.containsColon = containsColon;

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
    public long extractTimestampMillis(final String messagePayload, final String kafkaTopic) throws Exception {

        long result =0;
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
                            result = tsParseRs.date.getTime();
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
                    ts = new SimpleDateFormat(ParseableTsFormat).parse(parseableTsStr);
            } else{
                throw new Exception("No time stamp pattern found!");
            } // if no meaningful ts found, throw an exception
        } else { // if length not good, throw an exception
            throw  new Exception("time stamp length not good: " + tsString.length());
        }
        return new TsParseResult(ts, containsColon);
    }

}