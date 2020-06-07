package com.imooc.bigdata;

public interface ImoocMapper {
    /**
     * @param line    读取到每一行数据
     * @param context 上下文/缓存
     */
    public void map(String line, ImoocContext context);
}
