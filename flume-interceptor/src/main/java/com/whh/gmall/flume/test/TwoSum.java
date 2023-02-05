package com.whh.gmall.flume.test;

public class TwoSum {
    public static void main(String[] args) {
        TwoSum tw = new TwoSum();

        int[] t = tw.twoSum(new int[]{2, 3, 6}, 5);


        System.out.print("[");

        for (int i = 0;i < t.length ;i++ ) {

            if (i == t.length - 1) {
                System.out.println(t[i] + "]");
            } else {
                System.out.print(t[i] + ", ");
            }
        }
    }


    public int[] twoSum(int[] nums,int target){
        int l = nums.length;
        for(int i = 0;i < l;i++){
            for(int j = i + 1;j < l;j++){
                if(nums[i]+nums[j] == target){
                    return new int[]{i,j};
                }
            }
        }
        return new int[0];
    }
}
