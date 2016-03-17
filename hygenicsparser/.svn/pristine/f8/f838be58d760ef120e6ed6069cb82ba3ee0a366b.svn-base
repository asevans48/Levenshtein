package com.hygenics.distance;

/**
 * Levenshtein algorithm for use with the Spring tools deduplication step.
 * Pentaho is a by row tool and this kind of lookup is actually quite
 * slow. This tool can be used to group on ids and then perform 
 * Levenshtein on groups instead of having to accumulate and then
 * iterate across everything. Consider creating batch iterators
 * on the ids (likely the hashes).
 * 
 * 
 * Reference Function: https://en.wikipedia.org/wiki/Levenshtein_distance
 * 
 * @author aevans
 *
 */
public class Levenshtein {
	
	/**
	 * Compare 2 words using the functional matrix-like approach to the Levenshtein algorithm.
	 * Null = error. Approach is basd off of function provided by wikepedia. 
	 * 
	 * @param worda
	 * @param wordb
	 * @return
	 */
	public static int getLevenshteinDistance(String worda,String wordb){
		int[][] diags = null;
		
		if(worda.length() == 0){
			return wordb.length();
		}else if(wordb.length() ==0){
			return worda.length();
		}else if(wordb.length() ==1 && worda.length() ==1){
			return (worda.equals(wordb))?0 : 1;
		}else if(worda.length() == 1 || wordb.length() == 1){
			return Math.abs(worda.length() - wordb.length()) + ((worda.charAt(0) == wordb.charAt(0)) ? 0 : 1);
		}
		
		if(worda.length() > wordb.length()){
			String temp = worda;
			worda = wordb;
			wordb = temp;
		}
		
		diags = new int[worda.length() + 1][wordb.length() + 1];
		
		
		for(int i = 0; i <= wordb.length();i++){
			diags[0][i] = i;
		}
		
		for(int i = 0; i <= worda.length();i++){
			diags[i][0] = i;
		}
		
		
		for(int j=1; j <= wordb.length(); j++){
			for(int i = 1; i <= worda.length(); i++){
				int indicator = (worda.charAt(i - 1) == wordb.charAt(j - 1))? 0 : 1;//true false not necessarilly 0 or 1
				diags[i][j]=Math.min(Math.min(diags[i-1][j]+1,diags[i][j-1]),diags[i-1][j-1]+indicator);
			}
		}

		return (diags[worda.length()][wordb.length()]) + Math.abs(worda.length() - wordb.length());
	}
}
