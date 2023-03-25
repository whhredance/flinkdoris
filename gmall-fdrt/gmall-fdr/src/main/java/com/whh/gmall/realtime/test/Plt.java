package com.whh.gmall.realtime.test;

public class Plt {
    public static void main(String[] args) {
        int p = Plt.plt1(5);
        System.out.println(p);
    }

    public static int plt(int n){
        if (n == 1) {
            return 1;
        }


        //爬楼梯，一次只能1或2,动态规划
        //
        int[] dp = new int[n + 1];
        dp[1] = 1;
        dp[2] = 2;
        for (int i = 3; i <= n; i++) {
            dp[i] = dp[i - 1] + dp[i - 2];
        }
        return dp[n];
    }

    //递归，所处位置前只会是-1或-2
    public static int plt1(int n){
        if (n == 1||n ==2){
            return n;
        }else {
            return plt1(n - 1) + plt1(n - 2);
        }
    }

    //滚动数组
    static int plt2(int n){
        if (n == 1){
            return 1;
        }
        int first = 1;
        int second = 2;
        int third = 3;
        for (int i = 3; i <= n; i++) {
            third=second+first;
            first=second;
            second=third;
        }
        return second;
    }


}
