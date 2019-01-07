public class FirstPosition14 {
    public int binarySearch(int[] nums, int target) {
        // write your code here
        if (nums == null || nums.length == 0)
            return 0;
        else {
            int l = 0, r = nums.length - 1;
            int m = 0;
            while (l < r) {
                m = l + (r - l) / 2;
                if (target <= nums[m]) {
                    r = m;
                } else {
                    l = m + 1;
                }
            }
            if (nums[l] == target)
                return l;
            else
                return -1;
        }
    }

    public int Solution(int[] nums, int target) {
        // write your code here
        if (nums == null || nums.length == 0)
            return 0;
        else {
            return qs(nums, target, 0, nums.length - 1);
        }
    }

    public int qs(int[] nums, int target, int s, int e) {
        if (s >= e) {
            if (nums[s] == target)
                return s;
            else
                return -1;
        }
        int m = s + (e - s) / 2;
        if (nums[m] < target) {
            return qs(nums, target, m + 1, e);
        } else {
            return qs(nums, target, s, m);
        }
    }

    public static void main(String[] args) {

    }
}
