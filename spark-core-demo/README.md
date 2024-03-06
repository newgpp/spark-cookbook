### window spark hadoop 集成

1. 下载 Microsoft Visual C++ 2010 安装
   https://www.microsoft.com/zh-cn/download/details.aspx?id=26999
2. 下载 winutils
   https://github.com/steveloughran/winutils/releases/tag/tag_2017-08-29-hadoop-2.8.1-native
3. copy hadoop.dll 到 c/windows/system32
4. 配置 hadoop.home.dir
   System.setProperty("hadoop.home.dir", "F:\\Idea Projects\\spark-cookbook\\spark-core-demo\\ext");