package cbporganizer.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;
import java.util.List;

public interface FilesStorageService {

    void init();

    void save(MultipartFile file, String userId);

    void deleteFiles(String userId);

    List<String> getFiles(String userId);

    List<String> getFolders(String userId);

    List<String> getFilesInFolder(String userId, String folderName);

    byte[] getValidationResult(String userId, String folderName);
    
    String getCreateCsv(String userId, String folderName);

    byte[] getReport(String userId, String folderName) throws FileNotFoundException;

    String getReportAsString(String userId, String folderName);

   byte[] downloadCsv(String userId, String fileName);

}
