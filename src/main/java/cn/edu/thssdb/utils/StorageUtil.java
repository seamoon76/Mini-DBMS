package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.MakeFileException;
import cn.edu.thssdb.exception.ReadFromFileException;
import cn.edu.thssdb.exception.WriteToFileException;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class StorageUtil<V> {
  private String dir_name;
  private String file_name;
  private String full_path;

  enum Type {
    GZIP,
    NORMAL;
  }

  private Type type = Type.NORMAL;

  public StorageUtil(String dir_name, String file_name, boolean isGzip) {
    if (isGzip) {
      this.type = Type.GZIP;
    }
    this.dir_name = dir_name;
    this.file_name = file_name;
    this.full_path = Paths.get(dir_name, file_name).toString();
    File d = new File(this.dir_name);
    if (!d.isDirectory()) {
      d.mkdirs();
    }
    File f = new File(this.full_path);
    if (!f.isFile()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        throw new MakeFileException();
      }
    }
  }

  public void serialize(Iterator<V> iterator) {
    try {
      FileOutputStream fileOutputStream = new FileOutputStream(full_path);
      ObjectOutputStream o =
          this.type == Type.GZIP
              ? new ObjectOutputStream(new GZIPOutputStream(fileOutputStream))
              : new ObjectOutputStream(fileOutputStream);
      while (iterator.hasNext()) {
        o.writeObject(iterator.next());
      }
      o.flush();
      o.close();
    } catch (IOException e) {
      throw new WriteToFileException();
    }
  }

  public ArrayList<V> deserialize() {
    try {
      ArrayList<V> vs = new ArrayList<>();
      FileInputStream fileInputStream = new FileInputStream(full_path);
      ObjectInputStream o =
          this.type == Type.GZIP
              ? new ObjectInputStream(new GZIPInputStream(fileInputStream))
              : new ObjectInputStream(fileInputStream);
      while (true) {
        try {
          V obj = (V) o.readObject();
          vs.add(obj);
        } catch (EOFException e) {
          break;
        } catch (ClassNotFoundException e) {
          o.close();
          new File(this.full_path).delete();
          throw new ReadFromFileException();
        }
      }
      o.close();
      return vs;
    } catch (IOException e) {
      new File(this.full_path).delete();
      return new ArrayList<>();
    }
  }

  public void deleteFile() {
    File f = new File(this.full_path);
    f.delete();
  }
}
