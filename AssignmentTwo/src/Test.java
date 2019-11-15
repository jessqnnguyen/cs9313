import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test {

  public static void testCorrectOutput() throws IOException {
    BufferedReader br1 = null;
    BufferedReader br2 = null;
    String sCurrentLine;
    List<String> list1 = new ArrayList<>();
    List<String> list2 = new ArrayList<String>();
    br1 = new BufferedReader(new FileReader("correct.txt"));
    br2 = new BufferedReader(new FileReader("output/part-00000"));
    while ((sCurrentLine = br1.readLine()) != null) {
      list1.add(sCurrentLine);
    }
    while ((sCurrentLine = br2.readLine()) != null) {
      list2.add(sCurrentLine);
    }
    List<String> tmpList = new ArrayList<String>(list1);
    tmpList.removeAll(list2);
    if (tmpList.isEmpty()) {
      System.out.println("Files are exactly the same - test passed!");
    } else {
      System.out.println("content from test.txt which is not there in test2.txt");
      for(int i=0;i<tmpList.size();i++){
        System.out.println(tmpList.get(i)); //content from test.txt which is not there in test2.txt
      }

      System.out.println("content from test2.txt which is not there in test.txt");

      tmpList = list2;
      tmpList.removeAll(list1);
      for(int i=0;i<tmpList.size();i++){
        System.out.println(tmpList.get(i)); //content from test2.txt which is not there in test.txt
      }
    }
  }
  public static void main(String[] args) throws IOException {
    testCorrectOutput();
  }

}
