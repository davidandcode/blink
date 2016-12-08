package com.creditkarma.blink.utils.writer;

/**
 * Created by shengwei.wang on 12/6/16.
 */

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CkAutoTsMessageParser extracts ts field (specified by 'message.timestamp.name')
 * from JSON data and partitions data by date.
 * <p>
 * The ts field foramt is like "ts":"2015-01-07T14:22:35.924926-0800"
 * <p>
 * Created by bin.yu on 12/21/15.
 */
public class CkAutoTsMessageParser  {
    //private static final Logger LOG = LoggerFactory.getLogger(CkAutoTsMessageParser.class);

    static final String TimestampSampleMax = "2015-01-07T14:22:35.863513-08:00";
    static final String TimestampSampleMin = "2015-01-07T14:22:35-0800";
    static final int TimestampSampleMaxLength = TimestampSampleMax.length();
    static final int TimestampSampleMinLength = TimestampSampleMin.length();

    private static final Pattern UnparseableTsPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})(.?)(\\d{0,6}?)([+-]\\d{2})(:?)(\\d{2})");
    private static final String ParseableTsFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public static final int MaxRetryForSmartDetect = 5;
    private final boolean TimestampFormatWithMicrosecond;
    //private final boolean MessageTimestampParsingErrorLogEnabled;
    private final String enforcedParsingFields;

    // for each topic-column, keep true/false to indicate if need to convert it from ts to epoch
    private static ConcurrentHashMap<String, Boolean> needToParseColumns = new ConcurrentHashMap<String, Boolean>();
    // for each topic column, keep the test counter before making decision
    private static ConcurrentHashMap<String, AtomicLong> smartDetectColumns = new ConcurrentHashMap<String, AtomicLong>();

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

    public CkAutoTsMessageParser(String tsName,boolean ifWithMicro,String enforcedFields) {


        timestampName = tsName;
        TimestampFormatWithMicrosecond = ifWithMicro;
        //MessageTimestampParsingErrorLogEnabled = mConfig.getMessageTimestampParsingErrorLogEnabled();
        enforcedParsingFields = enforcedFields;
        //LOG.warn("TimestampFormatWithMicrosecond: " + TimestampFormatWithMicrosecond);
        //LOG.warn("enforcedParsingFields: " + enforcedParsingFields);
        //LOG.warn("MessageTimestampParsingErrorLogEnabled: " + MessageTimestampParsingErrorLogEnabled);

        geneticEnforcedParsedFields = new HashSet<String>();
        specialEnforcedParsedFields = new HashSet<String>();
        if(enforcedParsingFields != null) {
            for(String field : enforcedParsingFields.split(",")) {
                field = field.trim();
                if(field.length()>0) {
                    if(field.indexOf(".") >= 0) {
                        //LOG.warn("specialEnforcedParsedFields: " + field);
                        specialEnforcedParsedFields.add(field);
                    }
                    else {
                        //LOG.warn("geneticEnforcedParsedFields: " + field);
                        geneticEnforcedParsedFields.add(field);
                    }
                }
            }
        }
    }


    // this method may change input
    public TsParseResult extractTimestampMillis(final String[] messagePayload, final String kafkaTopic) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(messagePayload[0]);
        long tsColumnVal = 0;
        TsParseResult result = null;

        if (jsonObject != null) {


            // scan all the columns to re-format unparseable timestamp string
            boolean isPayloadChanged = false;
            for(String col: jsonObject.keySet()) {
                String tableNameAndColumnKey = kafkaTopic + "." + col;
                Object fieldValue = jsonObject.get(col);

                if (fieldValue != null && fieldValue instanceof String) {
                    String fieldStr = (String) fieldValue;
                    if(fieldStr != null && fieldStr.trim().length() > 0) {
                        Boolean columnStatus = needToParseColumns.get(tableNameAndColumnKey);
                        Boolean containsColon = null;
                        TsParseResult tsParseRs = null;
                        boolean enforcedParse = geneticEnforcedParsedFields.contains(col)
                                || specialEnforcedParsedFields.contains(tableNameAndColumnKey);

                        if (col.equals(timestampName) || columnStatus == null || columnStatus.booleanValue()
                                || enforcedParse) {
                            // sampling not done yet or it is a parseable column
                            // always parse tsEvent since it is hard to decide when to skip parse

                            tsParseRs = parseAsTsString(fieldStr);
                            if (columnStatus == null)
                                // sampling not done yet
                                increaseSamplingCounter(tableNameAndColumnKey, tsParseRs, enforcedParse);

                            if (tsParseRs.date != null && tsParseRs.containsColon != null && !tsParseRs.containsColon.booleanValue()) {
                                // convert to long number if it is a valid timestamp but does not contains colon in tz
                                isPayloadChanged = true;
                                // BQ acceptable format "1408452095.220"
                                jsonObject.put(col, String.format("%d.%d", tsParseRs.date.getTime() / 1000, tsParseRs.date.getTime() % 1000));
                            }
                        }

                        // get the value from special column "ts", which is the return value of this function
                        if (col.equals(timestampName) && tsParseRs != null && tsParseRs.date != null){
                            tsColumnVal = tsParseRs.date.getTime();
                            result = tsParseRs;
                        }
                    }
                }
            }

            if(isPayloadChanged) {
    try {
        messagePayload[0] = jsonObject.toJSONString();
    } catch(Exception e){

        // find a way to log this error
    }
                // re-format payload
                //try {
                    //message.setPayload(jsonObject.toJSONString().getBytes("UTF8"));
                //} //catch (UnsupportedEncodingException e) {
                   // LOG.error(String.format("error to change payload for event %s:", jsonObject), e);
                //}
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



                try {

                year = parseableTsStr.substring(0,4);
                month = parseableTsStr.substring(5,7);
                day = parseableTsStr.substring(8,10);
                hour = parseableTsStr.substring(11,13);
                minute = parseableTsStr.substring(14,16);
                second = parseableTsStr.substring(17,19);

//System.out.println(parseableTsStr + " " + hour + " " + minute + " " + second);
                    ts = new SimpleDateFormat(ParseableTsFormat).parse(parseableTsStr);
                } catch (Exception e) {
                    //if(MessageTimestampParsingErrorLogEnabled)
                        //LOG.warn(String.format("timestamp parsing error: %s with format %s; original: %s", parseableTsStr,
                          //      ParseableTsFormat, tsString));

                    throw e;
                }
            }
        }


        return new TsParseResult(ts, containsColon,year,month,day,hour,minute,second);
    }

    /**
     * Increase or decrease parsing counter. +1 for valid parsing; -1 for invalid parsing
     * Set the decision flag after counter reaches threshold MaxRetryForSmartDetect
     *
     * @param tableNameAndColumnKey
     * @param tsParseRs
     */
    static void increaseSamplingCounter(String tableNameAndColumnKey, TsParseResult tsParseRs, boolean enforcedParse) {
        if (smartDetectColumns.get(tableNameAndColumnKey) == null)
            smartDetectColumns.putIfAbsent(tableNameAndColumnKey, new AtomicLong(0));

        AtomicLong counter = smartDetectColumns.get(tableNameAndColumnKey);
        if (tsParseRs.date != null && !tsParseRs.containsColon || enforcedParse)
            counter.incrementAndGet();
        else
            counter.decrementAndGet();

        // set decision after parsing some events
        if (counter.get() >= MaxRetryForSmartDetect) {
            needToParseColumns.putIfAbsent(tableNameAndColumnKey, true);
            //LOG.warn("detected! table column will be converted from timestamp string to epoch: {}", tableNameAndColumnKey);
        }
        else if (counter.get() <= -MaxRetryForSmartDetect) {
            needToParseColumns.putIfAbsent(tableNameAndColumnKey, false);
            //LOG.warn("detected! table column will NOT be converted: {}", tableNameAndColumnKey);
        }
    }


    public static void main(String[] args) throws Exception{

        CkAutoTsMessageParser myParser = new CkAutoTsMessageParser("ts",true,"");

        System.out.println(myParser.parseAsTsString(TimestampSampleMax).day);


    }

}