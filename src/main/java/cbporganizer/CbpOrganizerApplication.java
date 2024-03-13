package cbporganizer;

import cbporganizer.service.FilesStorageService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;

@SpringBootApplication()
public class CbpOrganizerApplication implements CommandLineRunner {

	@Resource
	FilesStorageService storageService;

	public static void main(String[] args) {
		SpringApplication.run(CbpOrganizerApplication.class, args);
	}

	@Override
	public void run(String... arg) {
		storageService.init();
	}
}
