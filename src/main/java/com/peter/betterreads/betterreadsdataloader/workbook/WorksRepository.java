package com.peter.betterreads.betterreadsdataloader.workbook;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WorksRepository extends CassandraRepository<Works,String> {
    
}
