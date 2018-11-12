package regexptool;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.*;
import java.util.regex.*;
import java.io.*;

public class test {

	List<String> list = new ArrayList<String>();

	List<String> patternStringList = new ArrayList<String>();
	List<Integer> patternNumberList = new ArrayList<Integer>();

	Matcher matcher;
	String temp;
	Boolean isParsed = false;

	public void getPatternStringList(String fileName, String deviceType) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String tmp = null;
		String s = "";
		while ((tmp = br.readLine()) != null) {
			s = s + tmp;
		}
		br.close();
		// System.out.println(s);
		try {
			JSONObject dataJson = new JSONObject(s);
			JSONObject processors = dataJson.getJSONObject("processors");
			JSONObject deviceTypes = processors.getJSONObject(deviceType);
			JSONArray pattern_lists = deviceTypes.getJSONArray("pattern_list");
			for (int i = 0; i < pattern_lists.length(); i++) {
				JSONObject info = pattern_lists.getJSONObject(i);
				int id = info.getInt("pattern_id");
				String pattern = info.getString("pattern");
				System.out.println("pattern "+id + " ----> " + pattern);
				patternNumberList.add(id);
				patternStringList.add(pattern);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public List<String> getFieldsName(String patternString) {
		List<String> fields = new ArrayList<String>();
		Pattern pattern = Pattern.compile("(\\([^)]*\\))");
		Matcher matcher = pattern.matcher(patternString);
		while (matcher.find()) {
			Pattern subpattern = Pattern.compile("<(.*)>");
			Matcher submatcher = subpattern.matcher(matcher.group(1));
			if (submatcher.find()) {
				fields.add(submatcher.group(1));
			} else {
				fields.add("Unnamed");
			}
		}
		return fields;
	}

	public void ProcessFile(String fileName) {
		Scanner input = null;
		try {
			input = new Scanner(new File(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("Starting........\n");
		try {
			int linenum = 1;
			while (input.hasNext()) {
				temp = input.nextLine();
				System.out.println("line " + linenum + " ： " + temp );
				isParsed = false;
				int i;
				for (i = 0; i < patternStringList.size(); i++) {
					Pattern pattern = Pattern.compile(patternStringList.get(i));
					System.out.println("i = " + i);
					Matcher matcher = pattern.matcher(temp);
					System.out.println("if start .............");
					if (matcher.find()) {
						System.out.println("matcher.find() is true");
						isParsed = true;
						System.out.println("pattern " + patternNumberList.get(i) + " is used to parsed log's " + linenum
								+ " line right!");
						// System.out.println(matcher.groupCount());
						List<String> fields = getFieldsName(patternStringList.get(i));
						if(fields.size()==matcher.groupCount()){
							System.out.println(
									"________________________________________________________________________________");
							for (int j = 1; j <= matcher.groupCount(); j++) {
								System.out.printf("|%-19s|%-58s|\n", fields.get(j - 1), matcher.group(j));
							}
						}else{
							System.out.println("WARN: the fileds' amount is not equal to the matcher's groupcount!!");
							System.out.println(
									"________________________________________________________________________________");
							for (int j = 1; j <= matcher.groupCount(); j++) {
								System.out.printf("|%-19d|%-58s|\n", j, matcher.group(j));
							}
						}
						
						System.out.println(
								"--------------------------------------------------------------------------------\n");
						System.out.println("exit if .............");
					}else{
						System.out.println("matcher.find() is false");
					}
					System.out.println("if end .............");
					if (isParsed == true) {
						break;
					}
				}
				if (i == patternStringList.size()) {
					System.out.println("The " + linenum + "-th line can not be parsed, raw message is: " + temp);
					System.out.println(
							"--------------------------------------------------------------------------------\n");
				}
				linenum++;
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		/*if (args.length < 3) {
			System.err.println("使用方法：请传递三个参数。\n 第一参数：日志文件路径（HDFS文件系统）；第二个参数：日志源名称；第三个参数：ETL解析程序配置文件");
			System.exit(-1);
		}
		String logFilePath = args[0];
		String sourceName = args[1];
		String configFilePath = args[2];*/
		test filterData = new test();
	    //filterData.getPatternStringList(logFilePath, sourceName);
		//filterData.ProcessFile(configFilePath);
		filterData.getPatternStringList("d:\\Users\\SNOW\\Desktop\\json.txt", "bcm_engine");
		filterData.ProcessFile("d:\\Users\\SNOW\\Desktop\\log.txt");
	}
}
