package shortestEdit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ShortestEdit {

	public String getShortestEdit(String forWord) throws IOException {
		
		File lexicon = new File("/home/ec2-user/searchengine/src/words.txt");
		int editDistance = forWord.length();
		String bestWord = "";
		
		BufferedReader br = new BufferedReader(new FileReader(lexicon));
		String eachWord = "";
		
		while((eachWord = br.readLine())!=null) {
			int thisDistance = getEditDistance(forWord, eachWord);
		//	System.out.println("For word "+eachWord+ ":"+thisDistance);

			if(thisDistance < editDistance) {
				editDistance = thisDistance;
				bestWord = eachWord;
			}
		}
		
		//System.out.println("For word "+bestWord+ ":"+editDistance);
		br.close();
		System.out.println("best matched word is:"+bestWord);
		return bestWord;
	}
	
	public int getEditDistance(String s1, String s2) {
		//System.out.println("Computing for s2: "+s2);
		int l1, l2;
		l1 = s1.length();
		l2 = s2.length();
		
		int[][] m = new int[l1+1][l2+1];
		
		//m[l1][l2] = 0;
		
		for(int i = 1; i<=l1; i++) {
			m[i][0] = i;
		}
		
		for(int j = 1; j<=l2; j++) {
			m[0][j] = j;
		}
		
		printMatrix(m, l1, l2);
		for(int i = 1; i<=l1; i++) {
			for(int j = 1; j<=l2; j++) {
				printMatrix(m, l1, l2);

				int value1, value2, value3;
				value1 = m[i-1][j-1];

				if(s1.charAt(i-1) == s2.charAt(j-1)) {
					value1 += 0;
				}
				else {
					value1 += 1;
				}
				value2 = m[i-1][j]+1;
				value3 = m[i][j-1]+1;
				
				/*System.out.println(s1.charAt(i-1) + " " + s2.charAt(j-1));
				System.out.println("Value1: "+value1);
				System.out.println("Value2: "+value2);
				System.out.println("Value3: "+value3);
				System.out.println("Min value: "+min(value1, value2, value3));*/
				m[i][j] = min(value1, value2, value3);
			}
		}
		printMatrix(m, l1, l2);

		return m[l1][l2];
	}
	
	public int min(int val1, int val2, int val3) {
		if(val1<=val2 && val1<=val3)
			return val1;
		if(val2<=val1 && val2<=val3)
			return val2;
		if(val3<=val1 && val3<=val2)
			return val3;
		
		return 0;
	}
	
	public void printMatrix(int m[][], int l1, int l2) {
		return;
	/*	System.out.println("");
		for(int i = 0; i<=l1; i++) {
			for(int j = 0; j<=l2; j++) {
				System.out.print(m[i][j]+"\t");
			}
			System.out.println("");
		}*/
	}
}
