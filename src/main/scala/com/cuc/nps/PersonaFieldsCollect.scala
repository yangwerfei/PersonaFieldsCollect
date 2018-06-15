package com.cuc.nps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object PersonaFieldsCollect {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("nps pfc")
      /** package前注释掉 */
//      .master("local[2]")
      .getOrCreate()

    /** 调试阶段设定loglevel=WARN */
//    spark.sparkContext.setLogLevel("WARN")

    /**
      * 数据输入与输出路径
      * 依次为：
      *   1.KPI-2G,KPI-3G,KPI-4G
      *   2.客户提供字段
      *   3.xdr_s1_mme,xdr_s1u_http,mro_locate
      *   4.cfg_siteinfo_tdlte(室内场景,紧急场景)
      *   5.输出
      */
    val input_kpi_2g: String = "hdfs://192.168.61.165:9000/nps/nps-2G接通率.csv"
    val input_kpi_3g: String = "hdfs://192.168.61.165:9000/nps/nps-3G接通率.csv"
    val input_kpi_4g: String = "hdfs://192.168.61.165:9000/nps/nps-4G指标.csv"

    val input_up1 :String = "hdfs://192.168.61.165:9000/nps/NPS-立即导出-utf8.csv"
    val input_up2 :String = "hdfs://192.168.61.165:9000/nps/3月收入表-20180503014319-立即导出-utf8.csv"

    val input_xm: String = "hdfs://192.168.61.165:9000/nps/xdr_s1mme/*"
    val input_xh: String = "hdfs://192.168.61.165:9000/nps/xdr_s1http/*"
    val input_mr: String = "hdfs://192.168.61.165:9000/nps/mro_locate/*"

    val cfg_siteinfo_tdlte: String = "hdfs://192.168.61.165:9000/nps/cfg_siteinfo_tdlte"

    val output_kpi_2g: String = "hdfs://192.168.61.165:9000/output/kpi-2g"
    val output_kpi_3g: String = "hdfs://192.168.61.165:9000/output/kpi-3g"
    val output_kpi_4g: String = "hdfs://192.168.61.165:9000/output/kpi-4g"
    val output_xdr_mr: String = "hdfs://192.168.61.165:9000/output/xdr_mr"

    /** kpi_2g */
    spark.read
      .option("header", "true")
      .csv(input_kpi_2g)
      .withColumnRenamed("sd占用次数","sd")
      .withColumnRenamed("sd请求","sd_all")
      .withColumnRenamed("tch占用次数","tch")
      .withColumnRenamed("tch请求","tch_all")
      .createOrReplaceTempView("T_GSM")

    spark.sql("""
      SELECT
          lac  ,
          cast(ci AS INT) int_ci  ,
          /* 2G语音接通率 */
          round((sum(sd) / sum(sd_all)) * (sum(tch) / sum(tch_all)),
          4) * 100 2g_voi_conn
      FROM
          T_GSM
      GROUP BY
          lac  ,
          ci
      ORDER BY
          lac  ,
          int_ci
      """)
//          .show(50)
            .repartition(1)
            .write
            .format("csv")
            .option("header", "true")
            .csv(output_kpi_2g)

    /** kpi_3g */
    spark.read
      .option("header", "true")
      .csv(input_kpi_3g)
      .withColumnRenamed("rrc成功次数","rrc")
      .withColumnRenamed("rrc尝试次数","rrc_all")
      .withColumnRenamed("ps域rab成功次数","ps_rab")
      .withColumnRenamed("ps域rab尝试次数","ps_rab_all")
      .withColumnRenamed("cs域rab成功次数","cs_rab")
      .withColumnRenamed("cs域rab尝试次数","cs_rab_all")
      .createOrReplaceTempView("T_WCDMA")

    spark.sql("""
      SELECT
          rnc_id,
          wbts_id,
          /* 3G语音接通率 */
          round(sum(rrc)/sum(rrc_all)*((sum(ps_rab)+sum(cs_rab))/(sum(ps_rab_all)+sum(cs_rab_all))),4)*100 3g_voi_conn
      FROM
          T_WCDMA
      GROUP BY
          rnc_id,
          wbts_id
      ORDER BY
          rnc_id,
          wbts_id
      """)
//          .show(50)
            .repartition(1)
            .write
            .format("csv")
            .option("header", "true")
            .csv(output_kpi_3g)

    /** kpi_4g */
    spark.read
      .option("header", "true")
      .csv(input_kpi_4g)
      .withColumnRenamed("上行丢包数","up")
      .withColumnRenamed("上行PDCP包总数","up_all")
      .withColumnRenamed("下行丢包数","down")
      .withColumnRenamed("下行PDCP包总数","down_all")
      .createOrReplaceTempView("T_LTE")

    spark.sql("""
      SELECT
          lncel_enb_id,
          lncel_lcr_id,
          /* 无线接通率 */
          round(sum(rrc_succ)/sum(rrc_att)*(sum(erab_succ)/sum(erab_att)),
          4)*100 wl_conn,
          /* CSFB成功率 */
          round((sum(CSFB_REDIR_CR_ATT)+sum(CSFB_PSHO_UTRAN_ATT))/(sum(UE_CTX_SETUP_ATT_CSFB)+sum(UE_CTX_MOD_ATT_CSFB)),
          4)*100 csfb_succ,
          /* 小区上行丢包数 */
          round(sum(up)/sum(up_all),
          4)*100 ul_package_loss_live,
          /* 小区下行丢包数 */
          round(sum(down)/sum(down_all),
          4)*100 dl_package_loss_live
      FROM
          T_LTE
      GROUP BY
          lncel_enb_id,
          lncel_lcr_id
      ORDER BY
          lncel_enb_id,
          lncel_lcr_id
      """)
//          .show(50)
            .repartition(1)
            .write
            .format("csv")
            .option("header", "true")
            .csv(output_kpi_4g)



    /** 人口属性一 */
    spark.read
      .option("header", "true")
      .csv(input_up1)
      //.withColumnRenamed("月份","month")
      .withColumnRenamed("账期日期","payment_day")
      .withColumnRenamed("设备号码", "msisdn")
      .withColumnRenamed("客户星级", "starts")
      .withColumnRenamed("客户出生日期", "birthday")
      //.withColumnRenamed("业务大类","business_category")
      //.withColumnRenamed("终端品牌","a")
      //.withColumnRenamed("终端型号","b")
      //.withColumnRenamed("入网天数","days")
      .withColumnRenamed("在网时长", "duration")
      .withColumnRenamed("客户性别", "gender")
      .withColumnRenamed("套餐名称","2i2c")
      //      .withColumnRenamed("停机类型","d")
      //      .withColumnRenamed("是否在网","e")
      //      .withColumnRenamed("是否当期停机","f")
      .withColumnRenamed("协议类型","c_t")
      //      .withColumnRenamed("协议ID","h")
      //      .withColumnRenamed("协议订购时间","i")
      .withColumnRenamed("协议生效时间","contract_start")
      .withColumnRenamed("协议失效时间","contract_end")
      //      .withColumnRenamed("协议名字","l")
      //      .withColumnRenamed("协议终端","m")
      //      .withColumnRenamed("协议终端品牌","n")
      .createOrReplaceTempView("u1")

    /** 人口属性二 */
    spark.read
      .option("header","true")
      .csv(input_up2)
      .withColumnRenamed("账期月份","payment_day")
      .withColumnRenamed("电话号码","MSISDN")
      .withColumnRenamed("当月总收入","arpu")
      .withColumnRenamed("业务类型","bill_network")
      .createOrReplaceTempView("u2")

    val c1: DataFrame = spark.sql("""
      SELECT
          msisdn msisdn1,
          FLOOR(CAST(datediff(CURRENT_DATE,
                       (CASE
                           WHEN birthday LIKE '--' THEN NULL WHEN birthday LIKE '%-% %:%' THEN birthday
                           ELSE CONCAT(
                                        substring(birthday,1,4),'-',
                                        substring(birthday,5,2),'-',
                                        substring(birthday,7,2))
                                  END)) AS int)/365) age,
          gender,
          2i2c,
          starts,
          duration,
          (CASE WHEN c_t='--' THEN '是' ELSE '否' END) b_contract,
          (CASE WHEN 2i2c LIKE '%融合%' OR 2i2c LIKE '智慧沃家' THEN '是' ELSE '否' END) b_fusion,
          (CASE WHEN 2i2c LIKE '智慧沃家' THEN '是' ELSE '否' END) b_int_wo,
          (CASE WHEN c_t='--' THEN c_t WHEN c_t LIKE '%单卡%' THEN '单卡' ELSE '其他' END) contract_type,
          contract_start,
          contract_end
      FROM
        (SELECT *
         FROM
           (SELECT msisdn,
                   birthday,
                   gender,
                   duration,
                   starts,
                   2i2c,
                   c_t,
                   contract_start,
                   contract_end,
                   row_number() over(partition BY MSISDN
                          ORDER BY contract_end DESC) row_no
            FROM u1
            ) a
         WHERE row_no=1
         ) b
      """)
    //      c1.show(20)

    val c2: DataFrame = spark.sql("""
      SELECT
          MSISDN,
          (case
              when sum(arpu)<50 THEN 1
              ELSE 2
          END) package_type,
          /* 收入 */
          sum(arpu) arpu_t,
          concat_ws(',',collect_list(bill_network)) bill_network_t
      FROM
          u2
      GROUP BY
          msisdn
      """)
//    c2.show(20)


    /** mro_locate */
    val mroRDD: RDD[Array[String]] = spark.sparkContext
      .textFile(input_mr)
      .map(_.split("\t"))
    val schemaStringMro = "S_CELL_ID,XDR_IMSI,MR_TYPE,S_RSRP,S_RSRQ,ORIG_LON,ORIG_LAT,LONGITUDE,LATITUDE,PLANID," +
      "ds,hs,CITY"
    val fieldsMro: Array[StructField] = schemaStringMro.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schemaMro: StructType = StructType(fieldsMro)
    val rowMroRDD = mroRDD.map(attr =>
        Row(attr(0), attr(1), attr(2), attr(3), attr(4), attr(5), attr(6), attr(7),
        attr(8), attr(9), attr(10), attr(11), attr(12)))
    val mroDF: DataFrame = spark.createDataFrame(rowMroRDD, schemaMro)
    mroDF.createOrReplaceTempView("mro_locate")

    spark.read
      .option("header", "true")
      .csv(cfg_siteinfo_tdlte)
      .createOrReplaceTempView("cfg_siteinfo_tdlte")

    val mro: DataFrame = spark.sql(
      """
      SELECT
         m.xdr_imsi,
         /* 弱覆盖采样占比 */
         round(sum(CASE WHEN m.S_RSRP>=-105 THEN 1 ELSE 0 END)/count(*), 2)*100 s_rsrp1,
         round(sum(CASE WHEN m.S_RSRP>=-115
                   AND m.S_RSRP<-105 THEN 1 ELSE 0 END)/count(*), 2)*100 s_rsrp2,
         round(sum(CASE WHEN m.S_RSRP<-115 THEN 1 ELSE 0 END)/count(*), 2)*100 s_rsrp3,
         round(avg(m.S_RSRP), 2) avg_rspr,
         round(avg(m.S_RSRQ), 2) avg_rspq,
         /* 室内场景使用占比 */
         round(sum(CASE WHEN c.sitetype='INDOOR' THEN 1 ELSE 0 END)/count(*), 2)*100 indoor
      FROM
         mro_locate m
      LEFT JOIN
         cfg_siteinfo_tdlte c ON m.s_cell_id=c.eci
      GROUP BY
         m.xdr_imsi
        """)
//        mro.show(20)


    /** xdr_s1_mme */
    val s1mmeRDD: RDD[Array[String]] = spark.sparkContext
      .textFile(input_xm)
      .map(_.split("\t"))
    val schemaStringXsm = "SECI,IMSI,MSISDN,IMEI,PROCEDURE_TYPE,UE_S1RELEASE_TIME,UE_S1RELEASE_CAUSE,ORIG_LON," +
      "ORIG_LAT,LONGITUDE,LATITUDE,DS,HS,CITY"
    val fieldsXsm: Array[StructField] = schemaStringXsm.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schemaXsm: StructType = StructType(fieldsXsm)
    val rowXsmRDD = s1mmeRDD.map(attr =>
        Row(attr(0), attr(1), attr(2), attr(3), attr(4), attr(5), attr(6), attr(7),
        attr(8), attr(9), attr(10), attr(11), attr(12), attr(13)))
    val s1mmeDF: DataFrame = spark.createDataFrame(rowXsmRDD, schemaXsm)
    s1mmeDF.createOrReplaceTempView("xdr_s1_mme")

    val xsm: DataFrame = spark.sql(
      """
         SELECT
             s.IMSI,
             s.MSISDN,
             /* 紧急场景掉线次*/
             sum(CASE WHEN (s.PROCEDURE_TYPE = '21'
                             AND (s.ue_s1release_time IS NOT NULL)
                             AND s.ue_s1release_cause IN (20,23,24,28)) THEN 1 ELSE 0 END) emergency_drop
         FROM
             xdr_s1_mme s
         LEFT JOIN
             cfg_siteinfo_tdlte c ON (s.SECI=c.ECI
                                            AND c.SCENARIO LIKE '%医院%')
         GROUP BY
             s.IMSI,
             s.MSISDN
        """)
//    xsm.show(20)

    /** xdr_s1u_http */
    val s1httpRDD: RDD[Array[String]] = spark.sparkContext
      .textFile(input_xh)
      .map(_.split("\t"))
    val schemaStringXsh = "STARTTIME,ENDTIME,IMSI,MSISDN,IMEI,TAC,ECI,APP_TYPE,APP_SUBTYPE,APP_STATUS,L4_PROTOCAL," +
      "DATA_UL,DATA_DL,TCP_SYNACK_LATENCY,TCP_ACK_LATENCY,TCP_CONN_FLAG,FIRST_REQ_DELAY,FIRST_REQ_RESP_DELAY," +
      "FIRST_PACKET_LATENCY,LAST_CONTENT_LATENCY,HTTP_STATUS,HOST,URI,USER_AGENT,CONTENT_TYPE,RELOCATEIONURI," +
      "ORIG_LON,ORIG_LAT,LONGITUDE,LATITUDE,PLANID,DS,HS,CITY"
    val fieldsXsh: Array[StructField] = schemaStringXsh.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schemaXsh: StructType = StructType(fieldsXsh)
    val rowXshRDD = s1httpRDD.map(attr => Row(attr(0), attr(1), attr(2), attr(3), attr(4), attr(5), attr(6), attr(7),
      attr(8), attr(9), attr(10), attr(11), attr(12), attr(13), attr(14), attr(15), attr(16), attr(17), attr(18),
      attr(19), attr(20), attr(21), attr(22), attr(23), attr(24), attr(25), attr(26), attr(27), attr(28), attr(29),
      attr(30), attr(31), attr(32), attr(33)))
    val s1httpDF: DataFrame = spark.createDataFrame(rowXshRDD, schemaXsh)
    s1httpDF.createOrReplaceTempView("xdr_s1u_http")

    val xsh: DataFrame = spark.sql(
      """
        select
          MSISDN,
          /* 平均页面下载速率 */
          CASE
          WHEN sum(CASE
                      WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
                          THEN LAST_CONTENT_LATENCY
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
                                  THEN data_dl
                              ELSE 0
                              END) / (
                          sum(CASE
                                  WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
                                      THEN LAST_CONTENT_LATENCY
                                  ELSE 0
                                  END)
                          ), 2)
          ELSE 0
          END dl_rate_data,
          /* 支付业务http响应成功率 */
          CASE
          WHEN sum(CASE
                      WHEN APP_TYPE = 9
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN APP_TYPE = 9
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END p_http_re_succ,
          /* 支付业务http显示成功率 */
          CASE
          WHEN sum(CASE
                      WHEN APP_TYPE = 9
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND last_content_latency > 0)
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN APP_TYPE = 9
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END p_http_dis_succ,
          /* 支付业务http响应时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 9 AND http_status < 400 AND first_packet_latency > 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0)
                                      + nvl(first_req_delay, 0) + nvl(first_packet_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END p_http_re_latency,
          /* 支付业务http显示时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 9 AND http_status < 400 AND last_content_latency > 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND last_content_latency > 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0) +
                                      nvl(first_req_delay, 0) + nvl(last_content_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 9 AND http_status < 400 AND last_content_latency > 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END p_http_dis_latency,
          /* 即时通讯业务http响应成功率 */
          CASE
          WHEN sum(CASE
                      WHEN APP_TYPE = 1
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN APP_TYPE = 1
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END c_http_re_succ,
          /* 即时通讯业务http显示成功率 */
          CASE
          WHEN sum(CASE
                      WHEN APP_TYPE = 1
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND last_content_latency > 0)
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN APP_TYPE = 1
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END c_http_dis_succ,
          /* 即时通讯业务http响应时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 1 AND http_status < 400 AND first_packet_latency > 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0) +
                                      nvl(first_req_delay, 0) + nvl(first_packet_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND first_packet_latency > 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END c_http_re_latency,
          /* 即时通讯业务http显示时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 1 AND http_status < 400 AND last_content_latency > 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND last_content_latency > 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0) +
                                  nvl(first_req_delay, 0) + nvl(last_content_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 1 AND http_status < 400 AND last_content_latency > 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END c_http_dis_latency,
          /* 支付业务tcp成功率 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 9 AND L4_protocal = 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN APP_TYPE = 9 AND tcp_conn_flag = 0 AND L4_protocal = 0
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 9 AND L4_protocal = 0)
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END p_tcp_succ,
          /* 支付业务tcp时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 9 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 9 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 9 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END p_tcp_latency,
          /* 即时通讯业务tcp成功率 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 1 AND L4_protocal = 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN APP_TYPE = 1 AND tcp_conn_flag = 0 AND L4_protocal = 0
                                  THEN 1
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 1 AND L4_protocal = 0)
                                  THEN 1
                              ELSE 0
                              END) * 100, 2)
          ELSE NULL
          END c_tcp_succ,
          /* 即时通讯业务tcp时延 */
          CASE
          WHEN sum(CASE
                      WHEN (APP_TYPE = 1 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                          THEN 1
                      ELSE 0
                      END) > 0
              THEN round(sum(CASE
                              WHEN (APP_TYPE = 1 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                                  THEN (nvl(tcp_synack_latency, 0) + nvl(tcp_ack_latency, 0))
                              ELSE 0
                              END) / sum(CASE
                              WHEN (APP_TYPE = 1 AND tcp_conn_flag = 0 AND L4_protocal = 0)
                                  THEN 1
                              ELSE 0
                              END), 2)
          ELSE NULL
          END c_tcp_latency,
          /* 低速率采样占比（大包200KB）*/
          round(CASE
 		            WHEN (
 		            		sum(CASE
 		            				WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            					THEN 1
 		            				ELSE 0
 		            				END)
 		            		) > 0
 		            	THEN (
 		            			sum(CASE
 		            					WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            						THEN CASE
 		            								WHEN (
 		            										CASE
 		            											WHEN (
 		            													CASE
 		            														WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            															THEN LAST_CONTENT_LATENCY
 		            														ELSE 0
 		            														END
 		            													) > 0
 		            												THEN round((
 		            															CASE
 		            																WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            																	THEN data_dl
 		            																ELSE 0
 		            																END
 		            															) / (
 		            															(
 		            																CASE
 		            																	WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            																		THEN LAST_CONTENT_LATENCY
 		            																	ELSE 0
 		            																	END
 		            																) / 1000
 		            															), 2)
 		            											ELSE 0
 		            											END
 		            										) < 500000
 		            									THEN 1
 		            								ELSE 0
 		            								END
 		            					ELSE 0
 		            					END)
 		            			) / sum(CASE
 		            				WHEN (data_dl >= 200000 AND LAST_CONTENT_LATENCY > 0)
 		            					THEN 1
 		            				ELSE 0
 		            				END)
 		           ELSE 0
 		        END, 4) * 100 low_rate,
          /* 上网业务大类型 */
           sum(CASE
               WHEN (APP_TYPE = 9
                 OR (APP_TYPE = 15 AND APP_SUBTYPE IN
                    (3, 6, 8, 11, 14, 15, 16, 26, 64, 69, 70, 71, 81, 91, 92, 93, 97, 98, 101, 102, 104))
                 OR (APP_TYPE = 17 AND APP_SUBTYPE IN (2, 98)))
                    THEN 1
                    ELSE 0
                    END) shopping,
              sum(CASE
                  WHEN ((APP_TYPE = 2
                  AND APP_SUBTYPE IN
                      (2, 19, 34, 44, 45, 46, 47, 48, 53, 54, 56, 57, 58, 59, 60, 62, 63))
                  OR (APP_TYPE = 15 AND APP_SUBTYPE IN
                      (30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 76, 77, 78, 79, 80)))
                      THEN 1
                  ELSE 0
                  END) news,
              sum(CASE
                  WHEN (APP_TYPE = 8
                  OR (APP_TYPE = 15 AND APP_SUBTYPE IN (117, 118, 119, 120, 121, 122, 123, 124, 125, 126))
                  OR (APP_TYPE = 17 AND APP_SUBTYPE IN (32, 85)))
                      THEN 1
                  ELSE 0
                  END) games,
              sum(CASE
                  WHEN (APP_TYPE = 5
                  OR (APP_TYPE = 12 AND APP_SUBTYPE IN (2, 6, 7, 10, 11, 12, 15, 21, 22, 23, 25))
                  OR (APP_TYPE = 17 AND APP_SUBTYPE IN (30, 51, 99))
                  OR (APP_TYPE = 19 AND APP_SUBTYPE IN (52, 76, 111)))
                      THEN 1
                  ELSE 0
                  END) videos,
              sum(CASE
                  WHEN (APP_TYPE = 1
                  OR APP_TYPE = 3
                  OR APP_TYPE = 11
                  OR APP_TYPE = 13
                  OR APP_TYPE = 14
                  OR (APP_TYPE = 17 AND APP_SUBTYPE IN (3, 19, 28, 71, 76, 81, 92))
                  OR (APP_TYPE = 19 AND APP_SUBTYPE IN (18, 119)))
                      THEN 1
                  ELSE 0
                  END) socialities,
              sum(CASE
                  WHEN ((APP_TYPE = 2 AND APP_SUBTYPE NOT IN
                      (2, 19, 34, 44, 45, 46, 47, 48, 53, 54, 56, 57, 58, 59, 60, 62, 63))
                  OR APP_TYPE = 4
                  OR APP_TYPE = 6
                  OR APP_TYPE = 7
                  OR APP_TYPE = 10
                  OR (APP_TYPE = 12 AND APP_SUBTYPE NOT IN
                      (2, 6, 7, 10, 11, 12, 15, 21, 22, 23, 25))
                  OR (APP_TYPE = 15 AND APP_SUBTYPE NOT IN
                      (3, 6, 8, 11, 14, 15, 16, 26, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
                      46, 47, 64, 69, 70, 71, 76, 77, 78, 79, 80, 81, 91, 92, 93, 97, 98, 101, 102, 104, 117, 118,
                      119, 120, 121, 122, 123, 124, 125, 126))
                  OR APP_TYPE = 16
                  OR (APP_TYPE = 17 AND APP_SUBTYPE NOT IN
                      (2, 3, 19, 28, 30, 32, 51, 71, 76, 81, 85, 92, 98, 99))
                  OR APP_TYPE = 18
                  OR (APP_TYPE = 19 AND APP_SUBTYPE NOT IN (52, 76, 111)))
                      THEN 1
                  ELSE 0
                  END) others
          FROM
              xdr_s1u_http
          GROUP BY
              MSISDN
       """)
//    xsh.show(20)

    val xsm_mro: DataFrame = xsm
      .join(mro,xsm("imsi") === mro("xdr_imsi"),"left")
      .drop("xdr_imsi")

    c1.join(c2,c1("msisdn1") === c2("msisdn"), "left")
        .join(xsh,c1("msisdn1") === xsh("msisdn"), "left")
        .join(xsm_mro,c1("msisdn1") === xsm_mro("msisdn"), "left")
        .drop("msisdn")
        .withColumnRenamed("msisdn1","msisdn")
//        .show(20)
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .csv(output_xdr_mr)


    spark.stop()
  }
}
