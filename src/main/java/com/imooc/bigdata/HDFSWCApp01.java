package com.imooc.bigdata;

/**
 * 使用HDFS API完成word count统计
 * 需求：统计HDFS上的文件wc，然后将统计结果输出到HDFS
 *
 * 功能拆解
 * 1）读取HDFS上的文件     ==>HDFS API
 * 2）业务处理（词频统计）：
 *          对文件中的每一行数据都要进行业务处理（按照分隔符分隔）==>Mapper
 * 3）将处理结果缓存起来    ==>Context
 * 4）将结果输出到HDFS     ==>HDFS API
 */


public class HDFSWCApp01 {


    public void he(){

    }

    public static void main(String[] args) {
        // 1）读取HDFS上的文件     ==>HDFS API

    }
}
