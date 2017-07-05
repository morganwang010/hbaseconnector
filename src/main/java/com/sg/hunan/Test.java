package com.sg.hunan;
import com.sun.media.jfxmedia.logging.Logger;
import org.apache.flume.sink.hbase.*;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;


/**
 * Created by morgan on 2017/6/2.
 */
public  class Test implements AsyncHbaseEventSerializer {
    private byte[] rowKey;
    private byte[] table;
    private byte[] cf;
    private byte[] colNameBytes;
    private byte[] payload;
    private byte[] value;
    private Event currentEvent;
/*rowkey地市级供电单位编码.台区编码.采集时间.量测类型编码
    "地市级供点单位：由数据org_id确认；
    台区编码：采用数据所属台区的tg_no；
    表计ID：数据对应METER_ID；
    采集时间：用电信息数据采集时间，格式为MMDDHHYYYYmmss。"

*/  private final int payloadLen = 107;
    String s1 = "I|OGG.E_MP_READ_CURVE|2017-07-04 03:40:00.481779|2017-07-04T11:50:16.774000|00000000000000008864|2010000002117590|2016-05-02 00:00:00|1|43401|3|100010001000100010001000100010001000100010001000100010001000100010001000100010001000100010001000|4076.08|NULL|NULL|NULL|4076.12|NULL|NULL|NULL|4076.15|NULL|NULL|NULL|4076.18|NULL|NULL|NULL|4076.21|NULL|NULL|NULL|4076.23|NULL|NULL|NULL|4076.25|NULL|NULL|NULL|4076.28|NULL|NULL|NULL|4076.33|NULL|NULL|NULL|4076.38|NULL|NULL|NULL|4076.43|NULL|NULL|NULL|4076.48|NULL|NULL|NULL|4076.53|NULL|NULL|NULL|4076.58|NULL|NULL|NULL|4076.63|NULL|NULL|NULL|4076.68|NULL|NULL|NULL|4076.72|NULL|NULL|NULL|4076.76|NULL|NULL|NULL|4076.80|NULL|NULL|NULL|4076.84|NULL|NULL|NULL|4076.89|NULL|NULL|NULL|4076.95|NULL|NULL|NULL|4077.01|NULL|NULL|NULL|4077.05|NULL|NULL|NULL";
    String[] a = new String[payloadLen];
    private final String payloadColumnSplit = "\\^A";
    private final String mp_read_curve_t1 = "FPR"; //1，正向有功
    private final String mp_read_curve_t2 = "FQR"; //2，正向无功
    private final String mp_read_curve_t3 = "FQ"; //3，一象限无功
    private final String mp_read_curve_t4 = "QQ"; //4，四象限无功
    private final String mp_read_curve_t5 = "BPR"; //5，反向有功
    private final String mp_read_curve_t6 = "BQR"; //6，反向无功
    private final String mp_read_curve_t7 = "SQ"; //7，二象限无功
    private final String mp_read_curve_t8 = "TQ"; //8，三象限无功

    @Override
    public void configure(Context context) {
        // TODO Auto-generated method stub
        String columns = context.getString("ColumnNames", "");
        String[] cols = columns.split(",");

        System.out.println(columns);
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // TODO Auto-generated method stub

    }

    @Override
    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.cf = cf;
    }

   @Override
    public void setEvent(Event event) {
        // TODO Auto-generated method stub
        this.payload = event.getBody();
        this.currentEvent = event;

       Logger.logMsg(0,String.valueOf(a.length+"\r\n"+a[9]));

    }
   /**
    @Override
    public void setEvent(Event event) {
        String strBody = new String(event.getBody());
        String[] subBody = strBody.split(this.payloadColumnSplit);

        if (subBody.length == this.colNameBytes.length)
        {
            this.payload = new byte[subBody.length][];
            for (int i = 0; i < subBody.length; i++)
            {
                this.payload[i] = subBody[i].getBytes(Charsets.UTF_8);
                if ((new String(this.colNameBytes[i]).equals(this.rowSuffixCol)))
                {
                    // rowkey 前缀是某一列的值, 默认情况是mac地址
                    this.rowKey = subBody[i];
                }
            }
        }

    }
    */

    byte[][] getValues(){
        byte[][] value = new byte[payloadLen][];
        int i = 0;
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        for(byte b : payload){
            if(124 == b){
                byte[] val = new byte[buffer.position()];
                buffer.flip();
                buffer.get(val);
                value[i] = val;
                i+=1;
                buffer.clear();
            }else{
                buffer.put(b);
            }
        }

        byte[] val = new byte[buffer.position()];
        buffer.flip();
        buffer.get(val);
        value[i] = val;
        buffer.clear();


        rowKey = value[0];
        return value;
    }

    @Override
    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList<PutRequest>();

        String strBody = new String(currentEvent.getBody());
        this.a = strBody.split("\\|");
      /*  byte[][] values = getValues();

        //将字节数据转化字符串数组
        for(int j=0;j<values.length;j++){
            a[j] = new String(values[j]);
        }*/
        String measType=""; //定义量测类型，生成rowkey使用
        int lenType = Integer.parseInt(a[9]); //量测点数，24点，48点，96点不同值使用
        int measureType = Integer.parseInt(a[7]);//定义量测类型，生成rowkey使用
        switch(measureType){
            case 1:
                measType = mp_read_curve_t1;
                break;
            case 2:
                measType = mp_read_curve_t2;
                break;
            case 3:
                measType = mp_read_curve_t3;
                break;
            case 4:
                measType = mp_read_curve_t4;
                break;
            case 5:
                measType = mp_read_curve_t5;
                break;
            case 6:
                measType = mp_read_curve_t6;
                break;
            case 7:
                measType = mp_read_curve_t7;
                break;
            case 8:
                measType = mp_read_curve_t8;
                break;
        }


        try{

            switch (lenType){
                case 1 : //96点
                    converttime1(a,a[6],1,actions,measType);
                    break;

                case 2: //48点
                    converttime2(a,a[6],2,actions,measType);
                    break;

                case 3: //24点
                    converttime3(a,a[6],3,actions,measType);
                    break;
            }
        } catch (Exception e) {
            throw new FlumeException("Could not get row key!", e);
        }

        return actions;
    }

    @Override
    public List<AtomicIncrementRequest> getIncrements() {
        List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
        return actions;
    }

    @Override
    public void cleanUp() {
        // TODO Auto-generated method stub

    }
    public  String converttime3(String[] d,String s,int t,List actions,String measType){
        //2016-05-02 00:00:00
        //采集时间：用电信息数据采集时间，格式为MMDDHHYYYYmmss。"
        String[] b = s.split(" ");
        String[] c = b[0].split("-");
        s = c[1] + c[2];

        for(int i=0;i<24;i++){
            if(i<10){
            /*    System.out.print(s+"0"+String.valueOf(i)+c[0]+"\r\n");
                System.out.print(d[11+4*i]+"\r\n");*/
                rowKey = (s+"0"+String.valueOf(i)+c[0]+"0000"+measType).getBytes();
                value = d[11+4*i].getBytes();
                colNameBytes = d[5].getBytes();
                PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                actions.add(putRequest);
            }
            else{
               /* System.out.print(s+String.valueOf(i)+c[0]+"\r\n");
                System.out.print(d[11+4*i]+"\r\n");*/
                rowKey = (s+ String.valueOf(i)+c[0]+"0000"+measType).getBytes();
                value = d[11+4*i].getBytes();
                colNameBytes = d[5].getBytes();
                PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                actions.add(putRequest);
            }

        }

        //System.out.print(s);
        return s;
    }


    public  String converttime2(String[] d,String s,int t,List actions,String measType){
        //2016-05-02 00:00:00
        //采集时间：用电信息数据采集时间，格式为MMDDHHYYYYmmss。"
        String[] b = s.split(" ");
        String[] c = b[0].split("-");
        s = c[1] + c[2];

        for(int i=0;i<48;i++){
            if(i<20){
                if(i%2==0){
                    /*System.out.print(s+"0"+String.valueOf(i/2)+c[0]+"0000"+"\r\n");
                    System.out.print(d[11+2*i]+"\r\n");*/
                    rowKey = (s+"0"+String.valueOf(i/2)+c[0]+"0000"+measType).getBytes();
                    value = d[11+2*i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);

                }else
                {
                   /* System.out.print(s+"0"+String.valueOf(i/2)+c[0]+"3000"+"\r\n");
                    System.out.print(d[11+2*i]+"\r\n");*/
                    rowKey = (s+"0"+String.valueOf(i/2)+c[0]+"3000"+measType).getBytes();
                    value = d[11+2*i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }

            }
            else{
                if(i%2==0){
                  /*  System.out.print(s+String.valueOf(i/2)+c[0]+"0000"+"\r\n");
                    System.out.print(d[11+2*i]+"\r\n");*/
                    rowKey = (s+String.valueOf(i/2)+c[0]+"0000"+measType).getBytes();
                    value = d[11+2*i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }else
                {
                    /*System.out.print(s+String.valueOf(i/2)+c[0]+"3000"+"\r\n");
                    System.out.print(d[11+2*i]+"\r\n");*/
                    rowKey = (s+ String.valueOf(i/2)+c[0]+"3000"+measType).getBytes();
                    value = d[11+2*i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }
            }

        }

        //System.out.print(s);
        return s;
    }


    public  String converttime1(String[] d,String s,int t,List actions,String measType){
        //2016-05-02 00:00:00
        //采集时间：用电信息数据采集时间，格式为MMDDHHYYYYmmss。"
        String[] b = s.split(" ");
        String[] c = b[0].split("-");
        s = c[1] + c[2];
        for(int i=0;i<96;i++){
            if(i<40){
                if(i%4==0){
                    //System.out.print(s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"000"+"\r\n");
                    //System.out.print(d[11+i]+"\r\n");
                    rowKey = (s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"000"+measType).getBytes();
                    value = d[11+i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }else{
                    /*System.out.print(s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"00"+"\r\n");
                    System.out.print(d[11+i]+"\r\n");*/
                    rowKey = (s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"00"+measType).getBytes();
                    value = d[11+i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }

            }
            else{
                if(i%4==0){
                    /*System.out.print(s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"000"+"\r\n");
                    System.out.print(d[11+i]+"\r\n");*/
                    rowKey = (s+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"000"+measType).getBytes();
                    value = d[11+i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }else{
                   /* System.out.print(s+"0"+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"00"+"\r\n");
                    System.out.print(d[11+i]+"\r\n");*/
                    rowKey = (s+String.valueOf(i/4)+c[0]+String.valueOf(i%4*15)+"00"+measType).getBytes();
                    value = d[11+i].getBytes();
                    colNameBytes = d[5].getBytes();
                    PutRequest putRequest = new PutRequest(table,rowKey,cf,colNameBytes,value);
                    actions.add(putRequest);
                }
            }

        }
        //System.out.print(s);
        return s;
    }


}
