package com.realtech.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 
 * @author yuanxiaozhong
 * 读取配置文件数据
 */
public class PropsUtil {

	private Properties props;
	private InputStream input;
	
	public PropsUtil(){
		this.props = new Properties();
		this.input = this.getClass().getResourceAsStream("/config.properties");
		try {
			this.props.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * @param filepath
	 * @param key
	 * @return
	 * @throws IOException 
	 */
	public String getValueByKey(String key){
		return props.getProperty(key);
	}
}
