package cbporganizer.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.multipart.MultipartFile;



import java.io.*;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Comparator;





@Service
public class FilesStorageServiceImpl implements FilesStorageService{

    private final Path root = Paths.get("cbpFiles");
    private final String validationResultFileName = "validated_study.html";
    private final String csvFileName ="clinical.csv";

    @Override
    public void init() {
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize folder for upload!");
        }
    }

    @Override
    public void save(MultipartFile file, String userId) {
        Path userDir = getUserPath(userId);
        try {
            if (!Files.exists(userDir)) {
                Files.createDirectory(userDir);
            }
            Path filePath = userDir.resolve(file.getOriginalFilename());

            Files.copy(file.getInputStream(), filePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            extractGZip(filePath.toFile(), userId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Resolves the path to the user's directory by the userId.
     */
    private Path getUserPath(String userId) {
        return root.resolve(userId);
    }

    private void extractGZip(File inputFile, String userId) {
        Path userDir = getUserPath(userId);
        try {
            TarArchiveInputStream tarInput = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(inputFile)));

            TarArchiveEntry entry;
            while((entry = tarInput.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    new File(userDir.resolve(entry.getName()).toString()).mkdirs();
                } else {
                    byte[] buffer = new byte[1024];
                    File outFile = userDir.resolve(entry.getName()).toFile();
                    outFile.getParentFile().mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(outFile)) {
                        int len;
                        while ((len = tarInput.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Returns a list of files in the user's directory.
     */
    @Override
    public List<String> getFiles(String userId) {
        Path userDir = getUserPath(userId);
        List<String> ret = new LinkedList<>();
        try {
            ret = Files.walk(userDir)
                    .filter(Files::isRegularFile)
                    .map(file -> file.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Could not list the files!");
        } finally {
            return ret;
        }
    }

    @Override
    public List<String> getFolders(String userId) {
        Path userDir = getUserPath(userId);
        if (!Files.exists(userDir)) {
            return null;
        }
        List<String> ret = new LinkedList<>();
        try {
            ret = Files.walk(userDir, 1) //only one level for study names
                    .filter(path -> path.toFile().isDirectory())
                    .map(folder -> folder.getFileName().toString())
                    .collect(Collectors.toList());
            // remove the user's directory
            ret.remove(0);
        } catch (IOException e) {
            throw new RuntimeException("Could not list folders!");
        }
        return ret;
    }

    @Override
    public List<String> getFilesInFolder(String userId, String folderName) {
        Path folderPath = getUserPath(userId).resolve(folderName);
        List<String> ret = new LinkedList<>();
        try {
            ret = Files.list(folderPath)
                    .filter(Files::isRegularFile)
                    .map(file -> file.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Could not list the files in the folder!");
        }
        return ret;
    }

    @Override
    public byte[] getValidationResult(String userId, String folderName) {
        Path userDir = getUserPath(userId).resolve(folderName);
        String scriptName = "validateData.py";
        byte[] ret = null;
        try {
            // Create a temporary directory to extract the script
            Path tempDir = Files.createTempDirectory("python-script");

            // Extract the script to the temporary directory
            Path importerDir = tempDir.resolve("importer");
            Path scriptFile = importerDir.resolve(scriptName);
            copyScriptsToTempDir(importerDir);

            File outFileHtml = new File(userDir.toFile(), validationResultFileName);

            ProcessBuilder processBuilder = new ProcessBuilder("python3", scriptFile.toString(),
                    "-s", userDir.toFile().getAbsolutePath(), "-n", "-html", outFileHtml.getAbsolutePath());
            processBuilder.directory(tempDir.toFile());

            processBuilder.redirectError(new File(userDir.toFile(), "errorOut.txt"));

            Process p = processBuilder.start();
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                ret = Files.readAllBytes(outFileHtml.toPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }


@Override
public String getCreateCsv(String userId, String folderName) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String retRes ="";
    try {
        // Command to execute the Python script
        String pythonCommand = "python3";
        String pythonScriptPath = "cbpFiles/user-local/trial-match-utils/src/tools/prepare_clinical_data.py";

       // String pythonScriptPath = "src/main/resources/sample.txt";
        ///src/main/resources/trail-match-utils/src/tools/prepare_clinical_data.py
       
         File pythonScriptFile = new File(pythonScriptPath);


        //String filePath = System.getProperty("user.dir")+ "\\src\\main\\resources\\trial-match-utils\\src\\tools\\prepare_clinical_data.py";
        
        String filePath = System.getProperty("user.dir")+ "/cbpFiles";
        
        //
        // Create a File object
        File file = new File(filePath);
        
        String absolutePath = file.getAbsolutePath();
        System.out.println("Absolute Path: " + absolutePath);
       System.out.println("User Directory " + System.getProperty("user.dir") +"Done");


        //String userDir = System.getProperty("user.dir");

        // Print the user directory
        //System.out.println("User Directory: " + userDir);


//checking with validate.py
        Path userDir = getUserPath(userId).resolve(folderName);
        String scriptName = "prepare_clinical_data.py";
        Path tempDir = Files.createTempDirectory("python-script");

        // Extract the script to the temporary directory
        Path importerDir = tempDir.resolve("importer");
        Path scriptFile = importerDir.resolve(scriptName);
        copyScriptsToTempDir(importerDir);
        File outFileHtml = new File(userDir.toFile(), csvFileName);



//csvFileName


/*
        // Create a File object for the user directory
        File userDirFile = new File(userDir);

        // Check if the user directory exists and is a directory
        if (userDirFile.exists() && userDirFile.isDirectory()) {
            // Get the list of subdirectories within the user directory
            File[] subdirectories = userDirFile.listFiles(File::isDirectory);

            // Print out each subdirectory
            System.out.println("Subdirectories:");
            if (subdirectories != null) {
                for (File subdir : subdirectories) {
                    System.out.println(subdir.getName());
                }
            }
        } else {
            System.out.println("User directory does not exist or is not a directory.");
        }

        System.out.println("Done");

*/

/*

        File currentDir = new File(userDir);

        // Check if the directory exists and is a directory
        if (currentDir.exists() && currentDir.isDirectory()) {
            // Get the list of files and directories in the current directory
            File[] filesAndDirs = currentDir.listFiles();

            // Loop through each file/directory
            for (File fileOrDir : filesAndDirs) {
                // If it's a directory, print its name and recursively call this method
                if (fileOrDir.isDirectory()) {
                    System.out.println("Subdirectory: " + fileOrDir.getAbsolutePath());
                    //listAllSubdirectories(fileOrDir.getAbsolutePath());

                }
            }
        } else {
            System.out.println("Directory does not exist or is not a directory: " + userDir);
        }

*/

/*

       Queue<File> directoryQueue = new ArrayDeque<>();
        
        // Add the initial directory to the queue
        directoryQueue.add(new File(userDir));

        // Process directories until the queue is empty
        while (!directoryQueue.isEmpty()) {
            // Remove the first directory from the queue
            File currentDir = directoryQueue.poll();

            // Check if the directory exists and is a directory
            if (currentDir.exists() && currentDir.isDirectory()) {
                // Get the list of files and directories in the current directory
                File[] filesAndDirs = currentDir.listFiles();

                // Loop through each file/directory
                for (File fileOrDir : filesAndDirs) {
                    // If it's a directory, print its name and add it to the queue for processing
                    if (fileOrDir.isDirectory()) {
                        System.out.println("Subdirectory: " + fileOrDir.getAbsolutePath());
                        directoryQueue.add(fileOrDir);
                    }
                }
            } else {
                System.out.println("Directory does not exist or is not a directory: " + currentDir.getAbsolutePath());
            }
        }
*/


        //System.getProperty("user.dir")
        // Check if the file exists
        if (file.exists()) {
            System.out.println("File exists.");
            
            // Check if the file is readable
            if (file.canRead()) {
                System.out.println("File is readable.");
            } else {
                System.out.println("File is not readable.");
            }
        } else {
            System.out.println("File does not exist.");
        }
       
        
        // Arguments for the Python script
        String[] arguments = {
            "-s", "cbpFiles/user-local/"+folderName+"/data_clinical_sample.txt",
            "-p", "cbpFiles/user-local/"+folderName+"/data_clinical_patient.txt",
            "-st", "cbpFiles/user-local/"+folderName+"/meta_study.txt",
            "-o", "cbpFiles/user-local/"+folderName+"/clinical.csv"
        };

        // Command and arguments
        String[] command = new String[arguments.length + 2];
        command[0] = pythonCommand;
        //change here as well
        command[1] = scriptFile.toString();
        command[2] = arguments[0];
        command[3] = arguments[1];
        command[4] = arguments[2];
        command[5] = arguments[3];
        command[6] = arguments[4];
        command[7] = arguments[5];
        command[8] = arguments[6];
        command[9] = arguments[7];

        //System.arraycopy(arguments, 0, command, 1, arguments.length);
        System.out.println(arguments[0]);
        // Execute the Python script
        //System.out.printf("Arguments: %s%n", Arrays.toString(arguments));
        //System.out.printf("Final command: %s%n", Arrays.toString(command));

        StringBuilder stringBuilder = new StringBuilder();

        for (String str : command) {
            stringBuilder.append(str);
        }

        String result = stringBuilder.toString();
       // System.out.println(result);

    System.out.printf("Command", result);
        Map<String, String> env = System.getenv();

        // Print each environment variable
        for (Map.Entry<String, String> entry : env.entrySet()) {
            //System.out.printf(entry.getKey() + " : " + entry.getValue());
        }
        ProcessBuilder pb = new ProcessBuilder(command);
       //ProcessBuilder pb = new ProcessBuilder("python3", "src/main/resources/trial-match-utils/src/tools/prepare_clinical_data.py -s src/main/resources/trial-match-utils/src/tools/samples/data_clinical_sample.txt -p src/main/resources/trial-match-utils/src/tools/samples/data_clinical_patient.txt -st src/main/resources/trial-match-utils/src/tools/samples/meta_study.txt -o src/main/resources/trial-match-utils/src/tools/samples/clinical.csv");
      /* ProcessBuilder pb = new ProcessBuilder(
    "python3",
    "prepare_clinical_data.py",
    "-s",
    "samples/data_clinical_sample.txt",
    "-p",
    "samples/data_clinical_patient.txt",
    "-st",
    "samples/meta_study.txt",
    "-o",
    "samples/clinical.csv"
); */


     //pb.directory(new File(""));
    // pb.directory(new File("/Users/dheerajgavini/Desktop/UHN/cbpOrganizer/cbpOrganizer/cbporganizer-backend/"));


            File csvFile = new File("cbpFiles/user-local/"+folderName+"/clinical.csv");
            long fileSize = csvFile.length(); 
            System.out.printf("Clinical.csv File size before operation"+Long.toString(fileSize));

        Process process = pb.start();

        // Capture output from Python script
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            // Process output here if needed
            System.out.println(line);
            // Write output to the byte array stream
            outputStream.write(line.getBytes());
        }



            InputStream errorStream = process.getErrorStream();
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));
            StringBuilder errorStringBuilder = new StringBuilder();
            System.out.println("Error:");
            while ((line = errorReader.readLine()) != null) {
                System.out.println(line);
                errorStringBuilder.append(line).append("\n");
            }

        // Wait for the process to complete
        int exitCode = process.waitFor();

        if (exitCode == 0) {
            
              File csvFile2 = new File("cbpFiles/user-local/"+folderName+"/clinical.csv");
            fileSize = csvFile2.length(); // Size in bytes
            //System.out.printf("Python script executed successfully. Size of CSV File: %d", fileSize);
            //System.out.println(String.format("Python script executed successfully. Size of CSV File: %d", fileSize));
            //retRes="Success while executing: "+Long.toString(fileSize);
            retRes="Success";
        } else {
            System.out.println("Error, exit code is not zero");
            System.err.println("Error executing Python script. Exit code: " + exitCode);
            retRes="Error: "+ errorStringBuilder.toString();
        }
        
        // You can also handle errors by reading from process.getErrorStream()

    } catch (IOException | InterruptedException e) {
        e.printStackTrace();
    }
    finally{
        return retRes;
    }

    //return outputStream.toByteArray();
    //return retRes;
}




    private String[] listValidatorFileNames() {
        // todo: traverse the directory and get all the file names
        String[] fileNames = {"__init__.py",
                "allowed_data_types.txt",
                "cbioportal_common.py",
                "cbioportalImporter.py",
                "chromosome_sizes.json",
                "cropped_validation_report_template.html.jinja",
                "data_cna_pd_annotations.txt",
                "importOncokbDiscreteCNA.py",
                "importOncokbMutation.py",
                "libImportOncokb.py",
                "metaImport.py",
                "updateOncokbAnnotations.py",
                "validateData.py",
                "validateStudies.py",
                "prepare_clinical_data.py",
                "validation_report_template.html.jinja"};
        return fileNames;
    }

    private void copyScriptsToTempDir(Path tempDir) throws IOException {
        for (String fileName : listValidatorFileNames()) {
            copyToTempDir(tempDir, fileName);
        }
    }

    private void copyToTempDir(Path importerDir, String fileName) throws IOException {
        // Create the importer subdirectory if it doesn't exist
        Files.createDirectories(importerDir);

        Path scriptFile = importerDir.resolve(fileName);
        try (InputStream is = getClass().getResourceAsStream("/importer/" + fileName)) {
            Files.copy(is, scriptFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public byte[] getReport(String userId, String folderName) throws FileNotFoundException {
        Path userDir = getUserPath(userId).resolve(folderName);
        File outFileHtml = new File(userDir.toFile(), validationResultFileName);

        if (!outFileHtml.exists()) {
            throw new FileNotFoundException("Validation report not found! Please validate the data first.");
        }

        byte[] ret = null;
        try {
            ret = Files.readAllBytes(outFileHtml.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }

    @Override
    public String getReportAsString(String userId, String folderName) {
        String ret = "";
        Path userDir = getUserPath(userId).resolve(folderName);
        File outFileHtml = new File(userDir.toFile(), validationResultFileName);

        try {
            String htmlContent = StreamUtils.copyToString(outFileHtml.toURL().openStream(), StandardCharsets.UTF_8);
            ret = htmlContent;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }

    @Override
    public void deleteFiles(String userId) {
        Path userDir = getUserPath(userId);
        try {
            // Delete all files in the user's directory
            Files.walk(userDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            
            // Return success response
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    @Override
    public byte[] downloadCsv(String userId, String fileName) {
/*
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"data.csv\"");
        
        // Assume csvData is a string containing your CSV data
        String csvData = "Name, Age\nJohn, 30\nDoe, 25";
        
        OutputStream outputStream = response.getOutputStream();
        outputStream.write(csvData.getBytes());
        outputStream.flush();
        outputStream.close()

*/
        
       // String filePath = determineFilePath(userId, folderName);

    try {
        File file = new File("cbpFiles/user-local/"+fileName+"/clinical.csv");

        if (file.exists()) {
            byte[] bytes = Files.readAllBytes(file.toPath());
            return bytes;
        } else {
            System.err.println("File not found: " + fileName);
            return null;
        }
    } catch (IOException e) {
        System.err.println("Error reading file: " + e.getMessage());
        e.printStackTrace();
        return null;
    }
    }



    /*
        @Override
    public void deleteFiles(String userId) {
        Path userDir = getUserPath(userId);
        try {
            // Delete all files in the user's directory
            Files.walk(userDir)
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .forEach(File::delete);
            // Return success response
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
     */
}
