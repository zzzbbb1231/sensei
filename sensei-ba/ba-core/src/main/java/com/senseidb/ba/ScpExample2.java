package com.senseidb.ba;


public class ScpExample2 {
public static void main(String[] args) throws Exception {
  Process process = new ProcessBuilder("bash","-c", "scp vzhabiuk@dpatel-ld:/tmp/1.txt /home/vzhabiuk").redirectErrorStream(true).start();
  System.out.println(process.waitFor());
  /*SSHClient ssh = new SSHClient();
  // ssh.useCompression(); // Can lead to significant speedup (needs JZlib in classpath)
  ssh.addHostKeyVerifier(new HostKeyVerifier() {
    
    @Override
    public boolean verify(String hostname, int port, PublicKey key) {
      return true;
    }
  });
  ssh.connect("dpatel-ld");
  try {
      //ssh.authPassword("vzhabiuk", "Mashroom4>");
      ssh.authPublickey("vzhabiuk", ssh.loadKeys("/home/vzhabiuk/.ssh/vzhabiuk_at_linkedin.com_dsa_key", "Mashroom1>"));
      
      ssh.newSCPFileTransfer().download("/tmp/1.txt", new FileSystemFile("."));
  } finally {
      ssh.disconnect();
  }*/
}
}
