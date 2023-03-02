package com.whh.gmall.realtime.test;

public class Plt {
    public static void main(String[] args) {
        int p = Plt.plt(5);
        System.out.println(p);
    }

    public static int plt(int n){
        if (n == 1) {
            return 1;
        }


        //爬楼梯，一次只能1或2
        //
        int[] dp = new int[n + 1];
        dp[1] = 1;
        dp[2] = 2;
        for (int i = 3; i <= n; i++) {
            dp[i] = dp[i - 1] + dp[i - 2];
        }
        return dp[n];
    }


}
