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
    private static boolean TimestampFormatWithMicrosecond = false;



    // for each topic column, keep true/false to indicate if need to convert it from ts to epoch
    private static ConcurrentHashMap<String, Boolean> needToParseColumns = new ConcurrentHashMap<String, Boolean>();
    // for each topic column, keep the test counter before making decision
    private static ConcurrentHashMap<String, AtomicLong> smartDetectColumns = new ConcurrentHashMap<String, AtomicLong>();

    private static String timestampName = "ts";
    private static Set<String> geneticEnforcedParsedFields = new HashSet<>(); // without topic name
    private static Set<String> specialEnforcedParsedFields = new HashSet<>(); // topic.field

    static {
        geneticEnforcedParsedFields.add("ts");
        specialEnforcedParsedFields.add("crap.ts");
        needToParseColumns.put("crap.ts",true);
    }

    public static class TsParseResult {
        public final Date date;
        public final Boolean containsColon;


        public TsParseResult(Date date, Boolean containsColon) {
            this.date = date;
//            System.out.println(date.toString());
            this.containsColon = containsColon;
        }
    }

    public CkAutoTsMessageParser(String tsName,boolean tsWithMicrosecond) {

        timestampName = tsName;
        TimestampFormatWithMicrosecond = tsWithMicrosecond;

        geneticEnforcedParsedFields = new HashSet<String>();
        specialEnforcedParsedFields = new HashSet<String>();

    }


    public static long extractTimestampMillis(final String[] messagePayload,String topic, String partition) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(messagePayload[0]);
        long tsColumnVal = 0;

        if (jsonObject != null) {


            // scan all the columns to re-format unparseable timestamp string
            boolean isPayloadChanged = false;
            for(String col: jsonObject.keySet()) {
                String tableNameAndColumnKey = topic + "." + col;
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

                            //if (tsParseRs.date != null && tsParseRs.containsColon != null && !tsParseRs.containsColon.booleanValue()) {
                            if (tsParseRs.date != null) {
                                // convert to long number if it is a valid timestamp but does not contains colon in tz
                                isPayloadChanged = true;
                                // BQ acceptable format "1408452095.220"
                                jsonObject.put(col, String.format("%d.%d", tsParseRs.date.getTime() / 1000, tsParseRs.date.getTime() % 1000));
                            }
                        }

                        // get the value from special column "ts", which is the return value of this function
                        if (col.equals(timestampName) && tsParseRs != null && tsParseRs.date != null)
                            tsColumnVal = tsParseRs.date.getTime();
                    }
                }
            }

            if(isPayloadChanged) {

                messagePayload[0] = jsonObject.toJSONString();

            }
        }
        return tsColumnVal;
    }

    /**
     * Parse field value as timestamp string
     *
     * @param tsString
     * @return
     */
    public static TsParseResult parseAsTsString(String tsString) {
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
                else
                    parseableTsStr = m.group(1) + ".0" + m.group(4) + m.group(6);
                try {
                    ts = new SimpleDateFormat(ParseableTsFormat).parse(parseableTsStr);
                } catch (ParseException e) {
                    //if(MessageTimestampParsingErrorLogEnabled)
                    //  LOG.warn(String.format("timestamp parsing error: %s with format %s; original: %s", parseableTsStr,
                    //        ParseableTsFormat, tsString));
                }
            }
        }

        return new TsParseResult(ts, containsColon);
    }



    public static String[] parseAsTs(String tsString) {

        String year = null;
        String month = null;
        String day = null;
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
                    year = m.group(1).substring(0,4);
                    month = m.group(1).substring(5,7);
                    day = m.group(1).substring(8,10);
                }
                else
                    parseableTsStr = m.group(1) + ".0" + m.group(4) + m.group(6);
                year = m.group(1).substring(0,4);
                month = m.group(1).substring(5,7);
                day = m.group(1).substring(8,10);
            }
        }

        return new String[]{year,month};
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
            // LOG.warn("detected! table column will be converted from timestamp string to epoch: {}", tableNameAndColumnKey);
        }
        else if (counter.get() <= -MaxRetryForSmartDetect) {
            needToParseColumns.putIfAbsent(tableNameAndColumnKey, false);
            //LOG.warn("detected! table column will NOT be converted: {}", tableNameAndColumnKey);
        }
    }


    public static void main(String[] args){

        TsParseResult mResult = parseAsTsString(TimestampSampleMax);

        System.out.println(mResult.date.toString());


        String[] temp = {"{\"country\":\"United States\",\"appVersion\":\"4.3.1\",\"mobileVersion\":\"6.0.1\",\"screen\":\"ScoreDetails\",\"section\":\"CreditFactors\",\"source\":\"\",\"schemaName\":\"CreditClick.json\",\"deviceId\":\"601be996-97eb-4beb-907e-dce7ff67c833R\",\"userAge\":44,\"userTransunionScore\":587,\"factorName\":\"TotalAccounts\",\"rank\":5,\"state\":\"Georgia\",\"deviceCarrier\":\"T-Mobile\",\"contentType\":\"CreditFactors\",\"krsRecommendationId\":\"\",\"deviceType\":\"Samsung Galaxy Note 4\",\"traceId\":\"f3851f49-dc3e-4f49-96cf-3163fdf8f2b2\",\"userEquifaxScore\":607,\"accountType\":\"\",\"ipAddress\":\"172.58.152\",\"deviceFamily\":\"Samsung Galaxy Note\",\"taplyticsExperiments\":\"{\\\\\\\"cc_refi_cost_editing\\\\\\\":\\\\\\\"enabled\\\\\\\",\\\\\\\"android_account_access_improvements\\\\\\\":\\\\\\\"ShowAccountAccessImprovements\\\\\\\",\\\\\\\"android_fingerprint\\\\\\\":\\\\\\\"baseline\\\\\\\",\\\\\\\"android_regstep2_permission_copy_update\\\\\\\":\\\\\\\"variation_1\\\\\\\"}\",\"version\":\"f953754226a066a456c9de13870d99c8\",\"position\":2,\"deviceBrand\":\"samsung\",\"city\":\"Lithonia\",\"tradelineId\":\"\",\"tsEvent\":\"2016-12-05 16:51:56.419000\",\"contentId\":\"\",\"platform\":\"Android\",\"renderId\":0,\"factorRange\":\"7\",\"requestId\":0,\"mobileRequestId\":0,\"subScreen\":\"Overview\",\"accountCreditor\":\"\",\"deviceManufacturer\":\"samsung\",\"creditBureau\":\"TransUnion\",\"userRegistrationDate\":\"\",\"releaseVersion\":\"v16.48B.03\",\"userMetaDataId\":31662412,\"mobileOS\":\"android\",\"url\":\"\",\"eventCode\":\"viewFactorDetails\",\"mobileRenderId\":0,\"deviceModel\":\"SM-N910T\",\"browserWidth\":0,\"destinationScreen\":\"creditkarma:\\/\\/factors\\/details\",\"ts\":\"2015-01-07T14:22:35.863513-08:00\"}"};

        long result = extractTimestampMillis(temp,"crap","crap");

        System.out.println(result);

        System.out.println(temp[0]);


        String[] mResultnew = parseAsTs(TimestampSampleMax);

        for(String s:mResultnew){
            System.out.println(s);
        }

    }

}