import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Definition of TreeNode:
 * public class TreeNode {
 * public int val;
 * public TreeNode left, right;
 * public TreeNode(int val) {
 * this.val = val;
 * this.left = this.right = null;
 * }
 * }
 */

public class Solution {
    /**
     * This method will be invoked first, you should design your own algorithm
     * to serialize a binary tree which denote by a root node to a string which
     * can be easily deserialized by your own "deserialize" method later.
     */
    public static String serialize(TreeNode root) {
        // write your code here
        if (root == null)
            return "";
        StringBuilder ret = new StringBuilder();
        Queue<TreeNode> queue = new LinkedList<TreeNode>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            TreeNode t = queue.poll();
            if (t == null) {
                ret.append("#,");
            } else {
                ret.append(t.val).append(",");
                queue.offer(t.left);
                queue.offer(t.right);
            }
        }
        //return ret.toString();
        return ret.substring(0, ret.length() - 1);
    }

    /**
     * This method will be invoked second, the argument data is what exactly
     * you serialized at method "serialize", that means the data is not given by
     * system, it's given by your own serialize method. So the format of data is
     * designed by yourself, and deserialize it here as you serialize it in
     * "serialize" method.
     */
    public static TreeNode deserialize(String data) {
        // write your code here
        if (data.isEmpty() || data.equalsIgnoreCase("#"))
            return null;
        String[] ss = data.split(",");
        Queue<TreeNode> queue = new LinkedList<TreeNode>();
        TreeNode root = new TreeNode(Integer.parseInt(ss[0]));
        queue.offer(root);
        int i = 1;
        while (!queue.isEmpty() && i < ss.length) {
            TreeNode t = queue.poll();
            if (ss[i].equalsIgnoreCase("#")) {
                t.left = null;
            } else {
                t.left = new TreeNode(Integer.parseInt(ss[i]));
                queue.offer(t.left);
            }
            i++;
            if (ss[i].equalsIgnoreCase("#")) {
                t.right = null;
            } else {
                t.right = new TreeNode(Integer.parseInt(ss[i]));
                queue.offer(t.right);
            }
            i++;
        }
        return root;
    }

    public static List<Integer> searchRange(TreeNode root, int k1, int k2) {
        // write your code here
        if (k2 < k1 || root == null)
            return null;
        List<Integer> ret = new LinkedList<Integer>();
        Queue<TreeNode> queue = new LinkedList<TreeNode>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            TreeNode t = queue.poll();
            if (t.val > k2) {
                if (t.right != null)
                    queue.offer(t.right);
                if (t.left != null && t.left.val < k2)
                    queue.offer(t.left);
            } else if (t.val < k1) {
                if (t.left != null)
                    queue.offer(t.left);
                if (t.right != null && t.right.val > k1)
                    queue.offer(t.right);
            } else {
                ret.add(t.val);
                if (t.right != null)
                    queue.offer(t.right);
                if (t.left != null)
                    queue.offer(t.left);
            }
        }
        if(ret.size()>0){
            Collections.sort(ret);
        }
        return ret;
    }

    public static void main(String[] args) {
        TreeNode node1 = new TreeNode(20);
        TreeNode node2 = new TreeNode(8);
        TreeNode node3 = new TreeNode(22);
        node1.left = node2;
        node1.right = node3;
        TreeNode node4 = new TreeNode(4);
        TreeNode node5 = new TreeNode(12);
        node2.left = node4;
        node2.right = node5;

        String s = serialize(node1);
        System.out.println(s);

        String str = new String("1,#,2,#,3,#,4,#,5");
        //TreeNode root = new SerializeTree().deserialize(s);
        TreeNode root = deserialize(str);
        System.out.println(root==null?"y":"n");
        //System.out.println((int) root.val);
        /*System.out.println(root.left.val);
        System.out.println(root.right.val);
        System.out.println(root.left.left == null ? "#" : root.left.left.val);
        System.out.println(root.left.left == null ? "#" : root.left.right.val);
        System.out.println(root.right.left.val);
        System.out.println(root.right.right.val);*/

        List<Integer> r = searchRange(root, 2, 4);
        if(null!=r){
            for (Integer aR : r) {
                System.out.print(aR + " ");
            }
        }

    }
}