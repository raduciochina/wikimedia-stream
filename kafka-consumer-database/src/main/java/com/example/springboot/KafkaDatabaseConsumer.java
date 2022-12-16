package com.example.springboot;

import com.example.springboot.enitity.WikimediaData;
import com.example.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KafkaDatabaseConsumer.class);
    @Autowired
    private WikimediaDataRepository repository;

    public KafkaDatabaseConsumer(WikimediaDataRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "wikimedia", groupId = "wikimediaGroup")
    public void consume(String eventMessage){
        LOGGER.info("Received event message: {}", eventMessage);

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiData(eventMessage);

        repository.save(wikimediaData);
    }

}
