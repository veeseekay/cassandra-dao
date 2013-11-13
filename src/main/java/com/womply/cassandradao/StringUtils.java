package com.womply.cassandradao;

import org.apache.commons.lang.WordUtils;

public abstract class StringUtils extends org.apache.commons.lang.StringUtils {
	
	public static String camelcaseToUnderscores(String camelcase) {
		return removeEnd(camelcase.replaceAll("([A-Z][a-z]+)", "$1_"), "_");
	}
	
	public static String underscoresToCamelcase(String underscores) {
		return WordUtils.capitalizeFully(underscores, new char[] { '_' }).replaceAll("_", "");
	}

}
