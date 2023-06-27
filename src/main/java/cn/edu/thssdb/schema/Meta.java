package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.MakeFileException;
import cn.edu.thssdb.exception.MetaFileNotFoundException;
import cn.edu.thssdb.exception.ReadFromFileException;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Meta {
  String dir_name;
  String file_name;
  String full_path;

  public Meta(String dir_name, String file_name) {
    this.dir_name = dir_name;
    this.file_name = file_name;
    this.full_path = Paths.get(dir_name, file_name).toString();

    File dir = new File(this.dir_name);
    if (!dir.isDirectory()) {
      dir.mkdir();
    }
    File file = new File(this.full_path);
    if (!file.isFile()) {
      try {
        file.createNewFile();
      } catch (IOException ioe) {
        throw new MakeFileException();
      }
    }
  }

  public ArrayList<String[]> readFromFile() {
    ArrayList<String[]> content = new ArrayList<>();
    String line;
    try {
      // 创建File对象
      // File file = new File(full_path);

      // 打开输入流
      FileInputStream fileInputStream = new FileInputStream(full_path);
      InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      // 逐行读取文件内容
      while ((line = bufferedReader.readLine()) != null) {
        content.add(line.split(" "));
      }

      // 关闭输入流
      bufferedReader.close();
      inputStreamReader.close();
      fileInputStream.close();
    } catch (FileNotFoundException fnfe) {
      throw new MetaFileNotFoundException(full_path);
    } catch (IOException ioe) {
      throw new ReadFromFileException();
    }
    return content;
  }

  public void writeToFile(ArrayList<String> meta_content) {
    try {
      // 创建文件
      File file = new File(this.full_path);
      file.getParentFile().mkdirs();
      file.createNewFile();

      // 打开输出流
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "UTF-8");

      // 逐行写入文件
      for (String line : meta_content) {
        outputStreamWriter.write(line);
        outputStreamWriter.write(System.lineSeparator());
      }

      // 关闭输出流
      outputStreamWriter.close();
      fileOutputStream.close();
    } catch (IOException ioe) {
      throw new MakeFileException();
    }
  }

  public void deleteFile() {
    File file = new File(this.full_path);
    file.delete();
  }
}
