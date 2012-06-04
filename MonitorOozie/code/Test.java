//package com.ionelgog;

public class Test {
    
    public static void main(String[] args) {
        String[] cmd = new String[3];
        cmd[0] = "/bin/bash";
        cmd[1] = "-c";
        cmd[2] = "sleep 100";
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
        } catch (Exception e) {
        }
    }
}