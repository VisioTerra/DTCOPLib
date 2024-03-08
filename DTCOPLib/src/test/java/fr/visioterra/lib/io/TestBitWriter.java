package fr.visioterra.lib.io;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

public class TestBitWriter {
	
	public static void main(String[] args) throws Exception {
		File file = new File("D:/temp/COPEX-DCC/kokoriko.bin");
		
		try (BitWriter bitWriter = new BitWriter(new FileOutputStream(file), 1)) {
			
			
			int[] codes = new int[] { 186584777, -2049094063, 655468196, -2109322649, 768836872, -438398844, -920609232, -362183593, -1363319748, 2107998393};
			int[] codesLen = new int[] { 21, 12, 25, 23, 13, 13, 7, 19, 24, 24};
			
			Random random = new Random();
			
			for (int i = 0; i < 10; i += 1) {
//				int code = random.nextInt();
//				int codeLen = (int) (random.nextDouble() * 16);
				
				int code = codes[i];
				int codeLen = codesLen[i];
				
				bitWriter.writeBits(code, codeLen);
				
				String binaryString = Integer.toBinaryString(code & ((1 << codeLen) - 1));
				String leftPaddedBinaryString = String.format("%"+codeLen+"s", binaryString).replace(" ", "0");
				
				System.out.println(code + " (" + codeLen + ") - " + leftPaddedBinaryString);
			}
		}
	}

}
