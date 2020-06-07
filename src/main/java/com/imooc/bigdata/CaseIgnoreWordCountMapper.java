package com.imooc.bigdata;

public class CaseIgnoreWordCountMapper implements ImoocMapper {

    /**
     * @param line    读取到每一行数据
     * @param context 上下文/缓存
     */
    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.toLowerCase().split(" ");

        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v + 1);
            }
        }
    }
}
