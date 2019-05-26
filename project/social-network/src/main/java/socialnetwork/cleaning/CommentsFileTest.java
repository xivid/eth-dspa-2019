package socialnetwork.cleaning;

import socialnetwork.util.Config;
import socialnetwork.util.Helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;

public class CommentsFileTest {

    public static void main(String[] args) throws Exception {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String[] cleanedFileNames = Config.getStreamCleanedInputFiles();
        String[] rawFilesNames = Config.getStreamRawInputFiles();
        final BufferedWriter commentsOutputFile = Helpers.getFileWriter(cleanedFileNames[0]);
        final BufferedReader commentsInputFile = Helpers.getFileReader(rawFilesNames[0]);

        assert commentsInputFile != null;
        assert commentsOutputFile != null;

        commentsInputFile.readLine();
        commentsOutputFile.write("id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId\n");
        String line;
        int lineCount = 0;
        while ((line = commentsInputFile.readLine()) != null) {
            if(++lineCount % 1000000 == 0) {
                System.out.println("lines read = " + lineCount);
                commentsOutputFile.flush();
            }

            commentsOutputFile.write(line);
            commentsOutputFile.newLine();
        }

        commentsOutputFile.close();
        commentsInputFile.close();
    }
}
