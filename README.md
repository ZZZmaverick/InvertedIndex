# InvertedIndex
云计算期末设计——基于Hadoop平台的文字处理MapReduce程序


  本MapReduce程序实现了文字处理中的inverted index问题，能够设置停止词过滤指定字符串，能够得到有某单词出现的所有文件的文件名，该词在某个文件内的词频，该词在某个文件内的出现位置。
  


代码设计

Mapper

Mapper的目的是得到如下形式的结果：
<<key1, value1>, <key2, value2>, <key3, value3>>
其中单个<key, value>的内容是：
<单词, 出现该单词的这个文件名+该单词在此文件中本次出现的位置>
	Mapper中，先读取文件，进行分词处理，然后遍历词表进行词频的统计和出现位置的记录，此处的单词出现位置使用该单词在文件单词中的序数（出现顺序）表示。输出时，单词为key，value为出现该单词的这个文件名+该单词在此文件中本次出现的位置。相关代码和详细注释见InvertedIndex.java
  

停用词设计

停用词文件保存在stop_words中，在main里完成文件的读取和字符串处理转换，并使用config.set()进行传值。在Reducer中，使用config.get()获取停用词，并进行停用词的相关判断，key为停用词时不进行后续reduce操作。相关代码和详细注释见InvertedIndex.java
  

Reducer

Reducer的工作是完成停止词的判定和Mapper输出值的处理。停止词的判定方面，前面已经完成说明，此处不再赘述。Mapper输出值的处理上，目的是对每个单词(key)得到如下形式的输出：
单词 <出现该单词的文件A，该单词在文件A中的词频，[该单词在文件A中的出现位置]>;<出现该单词的文件B，该单词在文件B中的词频，[该单词在文件B中的出现位置]>……
	例如：apple	<file_3.txt, 1, [1]>; <file_2.txt, 1, [0]>; <file_1.txt, 1, [0]>
	具体的设计上，使用两个hashmap完成相关数据的记录。countMap用于记录文件名和在此文件中的词频，posMap用于记录文件名和在此文件中的出现位置。步骤上，先对输入value值进行拆分，获得文件名和出现位置，先依据文件名更新词频记录countMap（加一），后更新在此文件中的出现位置记录（使用String）。以上工作完成后，进行输出格式的处理。相关代码和详细注释见InvertedIndex.java


Main

Main中先进行停止词内容的文件读取和字符串处理并使用conf.set进行传值，之后创建job对象，完成相关类的参数设置、输入输出的路径设置等。相关代码和详细注释见InvertedIndex.java
  


具体内容和运行截图见课程设计报告
