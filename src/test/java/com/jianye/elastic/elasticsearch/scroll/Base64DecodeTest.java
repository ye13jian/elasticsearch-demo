/**
 * Project Name:elasticsearch-demo
 * File Name:Base64DecodeTest.java
 * Package Name:com.jianye.elastic.elasticsearch.scroll
 * Date:2017年7月25日上午11:22:16
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.scroll;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Base64;
import org.junit.Test;

/**
 * ClassName:Base64DecodeTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年7月25日 上午11:22:16 <br/>
 * 
 * @author admin
 * @version
 * @since JDK 1.6
 * @see
 */
public class Base64DecodeTest {

	@Test
	public void testDecode() throws IOException {
		
		String scrollId = "cXVlcnlUaGVuRmV0Y2g7NTsxMTU0OmgxMkh0RzVaUnFTYjdlVGVVekFfcXc7MTE1MzpoMTJIdEc1WlJxU2I3ZVRlVXpBX3F3OzExNTY6aDEySHRHNVpScVNiN2VUZVV6QV9xdzsxMTU1OmgxMkh0RzVaUnFTYjdlVGVVekFfcXc7MTE1NzpoMTJIdEc1WlJxU2I3ZVRlVXpBX3F3OzA7";
		BytesRef scroll_id_bytes = new BytesRef();
		scroll_id_bytes.bytes = Base64.decode(scrollId);
		CharsRef chars = new CharsRef();
		UnicodeUtil.UTF8toUTF16(scroll_id_bytes.bytes, 0, scroll_id_bytes.bytes.length, chars.chars);
		System.out.println("================");
		StringBuffer sb = new StringBuffer();
		for (char c : chars.chars) {
			sb.append(c);
		}
		System.out.println(sb);
		System.out.println("================");
	}
}
