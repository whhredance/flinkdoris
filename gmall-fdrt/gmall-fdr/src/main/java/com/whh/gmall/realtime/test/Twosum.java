package com.whh.gmall.realtime.test;

import java.util.HashMap;
//返回数组下标
public class Twosum {
    public static void main(String[] args) {
        int[] a = {1, 2, 4, 5};
        int b = 4;

//1使用对象调用方法
//        Twosum t = new Twosum();
//        int[] twosum = t.twosum(a, b);

//2静态方法直接使用类名调用（3构造方法直接new类名
        int[] twosum = Twosum.twosum(a, b);

        //遍历打印
        System.out.print("[");
        for (int i = 0;i < twosum.length ;i++ ) {
            if (i == twosum.length - 1) {
                System.out.println(twosum[i] + "]");
            } else {
                System.out.print(twosum[i] + ", ");
            }
        }

    }

    public static int[] twosum(int[] nums, int target){
        //hashmap，值放入（kv颠倒）[]中v作为map中key，
        // 每一次生成中间值（目标减遍历值），判断map中是否包含这个key
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            int complement = target - nums[i];
            if (map.containsKey(complement)) {
                return new int[]{map.get(complement),i};
            }
            map.put(nums[i],i);
        }
        throw new IllegalArgumentException("No two sum solution");
    }
}
