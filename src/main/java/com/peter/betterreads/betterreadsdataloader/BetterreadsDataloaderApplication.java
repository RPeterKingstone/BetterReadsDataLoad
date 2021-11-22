package com.peter.betterreads.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.datastax.oss.driver.api.core.data.Data;
import com.peter.betterreads.betterreadsdataloader.*;
import com.peter.betterreads.betterreadsdataloader.author.Author;
import com.peter.betterreads.betterreadsdataloader.author.AuthorRepository;
import com.peter.betterreads.betterreadsdataloader.workbook.Works;
import com.peter.betterreads.betterreadsdataloader.workbook.WorksRepository;
import com.peter.betterreads.connection.DataStaxAstraConfig.DataStaxAstraConfig;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraConfig.class)
@ComponentScan(basePackages = "com.peter.betterreads.*")
public class BetterreadsDataloaderApplication {

	@Autowired AuthorRepository authorRepository;

	@Autowired WorksRepository workRepository;

	@Value("${datadump.location.author}")
	private String authorDump;

	@Value("${datadump.location.works}")
	private String worksDump;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataloaderApplication.class, args);
	
	}

	public void initAuthors(){
		System.out.println("NOW ADDED TO CHECK GIT COMMIT");
		Path path = Paths.get(authorDump);
		try {
			Stream<String> lines = Files.lines(path);
			int count = 0;
			AtomicReference<Integer> counter = new AtomicReference<>(0);
			lines.skip(2336303).forEach(line -> {
				counter.getAndUpdate(value -> value + 1);
				String author_json = line.substring(line.indexOf("{"));
					try {
						JSONObject json_obj = new JSONObject(author_json);
						Author author = new Author();
						author.setName(json_obj.optString("name"));
						author.setPenName(json_obj.optString("personal_name"));
						author.setId(json_obj.optString("key").replace("/authors/", ""));
						System.out.println("Line no="+counter.get()+" ***  Saving Author "+author.getName()+"......");
						authorRepository.save(author);

						

						/*if((author.getName().equals("Peter Machina"))||(author.getName().equals("Manuel Fraire"))){
							System.out.println("Line no="+counter.get()+"Printing Author "+author.getName()+"......"+author.getId());
						}*/
						
						

					} catch (JSONException e) {
						e.printStackTrace();
					}

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void initWorks(){
		Path path = Paths.get(worksDump);
		DateTimeFormatter datePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try {
			Stream<String> lines = Files.lines(path);
			int count = 0;
			AtomicReference<Integer> counter = new AtomicReference<>(0);
			lines.forEach(line -> {
				counter.getAndUpdate(value -> value + 1);
				String work_json = line.substring(line.indexOf("{"));
					try {
						JSONObject json_obj = new JSONObject(work_json);
						Works book = new Works();
						book.setId(json_obj.optString("key").replace("/works/", ""));
						book.setName(json_obj.optString("title"));
						JSONObject descriptionObj = json_obj.optJSONObject("description");
						if(descriptionObj!=null)
						book.setDescription(descriptionObj.optString("value"));
						JSONObject createdObj = json_obj.optJSONObject("created");
						if(createdObj!=null){
							String date = createdObj.getString("value");
							book.setPublishedDate(LocalDate.parse(date,datePattern));
						}

						JSONArray covers_array = json_obj.optJSONArray("covers");
						if(covers_array!=null){
							List<String> coverIdList = new ArrayList<String>();
							for(int i=0;i<covers_array.length();i++){
								coverIdList.add(covers_array.getString(i));
							}
							
							book.setCoverIds(coverIdList);
						}
						JSONArray author_array = json_obj.optJSONArray("authors");
						if(author_array!=null){
							List<String> authorIds = new ArrayList<String>();
							for(int i=0;i<author_array.length();i++){
								JSONObject author = author_array.getJSONObject(i);
								String authorId = author.getJSONObject("author").getString("key").replace("/authors/", "");
								authorIds.add(authorId);
							}
							book.setAuthorIds(authorIds);
							List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id)).
							map(optionalAuthor -> {
								if(!optionalAuthor.isPresent()){
									return "Author Unknown";
								}
								else
								 return optionalAuthor.get().getName();
							}).collect(Collectors.toList());
							book.setAuthorNames(authorNames);
						}

						System.out.println("Saving book with id --- "+book.getName());
						workRepository.save(book);
					

					} catch (JSONException e) {
						e.printStackTrace();
					}

					

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
public void start(){
	initAuthors();
	//initWorks();
}

	

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraConfig config){
		Path bundle = config.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
