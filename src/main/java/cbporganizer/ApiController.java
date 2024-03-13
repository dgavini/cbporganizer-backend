package cbporganizer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/check")
public class ApiController {

	@PreAuthorize("hasRole('cbpuser')")
	@GetMapping(path = "/cbpuser")
	public String users() {
		return "You have cbp user privilages";
	}
}
